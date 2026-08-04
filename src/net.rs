//! Network implementation of the iroh-docs protocol

use std::{
    future::Future,
    sync::{Arc, OnceLock},
};

use iroh::{Endpoint, EndpointAddr, PublicKey};
use n0_future::time::{self, Duration, Instant};
use serde::{Deserialize, Serialize};
use tracing::{debug, error_span, trace, Instrument};

use crate::{
    actor::SyncHandle,
    metrics::Metrics,
    net::codec::{run_alice, BobState},
    NamespaceId, SyncOutcome,
};

/// The ALPN identifier for the iroh-docs protocol
pub const ALPN: &[u8] = b"/iroh-sync/1";

mod codec;

/// Bound on one whole sync exchange, connection establishment included.
///
/// Nothing below this point carries a timeout of its own, so a peer that
/// stays connected and stops talking stalls forever. Two things ride on the
/// bound. The live actor tracks one running exchange per namespace and peer
/// and refuses to start another while one runs, so a stalled exchange
/// blocks that pair and drops every later sync trigger silently. And a
/// session holds a store snapshot, whose read transaction holds back
/// reclamation of every page freed while it lives — of the oldest live one,
/// so concurrent sessions cost the same window as a single one, and the
/// window is this bound.
///
/// The bound is on the exchange as a whole, not on the wait between
/// messages, and there is no shorter liveness bound beside it. A peer
/// sending one message every few seconds defeats a between-messages bound
/// while holding both resources. A connection that goes dead rather than
/// quiet — a phone entering a tunnel — is already cut below, by QUIC keep
/// alives against its idle timeout, well inside this bound. And a
/// between-messages bound could not tell a slow transfer from silence
/// anyway: a message is delivered whole, so a peer sending one large
/// message for minutes looks exactly like a peer sending nothing.
///
/// The value covers a first sync of a large store over a slow link: an
/// entry is roughly 280 bytes on the wire, and a peer holding nothing
/// receives the served set in one message, so 10,000 entries are about 2.8
/// MB — five minutes carry that from about 75 kbit/s up. Beyond that the
/// exchange cannot complete at all rather than completing slowly, because a
/// message is ingested whole or not at all: a cut mid-message delivers
/// nothing, and the next session starts over. Raising the bound moves that
/// cliff; only bounding the size of a transmitted set removes it.
pub const SYNC_SESSION_TIMEOUT: Duration = Duration::from_secs(300);

/// Connect to a peer and sync a replica.
///
/// `filter` is this side's egress filter for the session: what the dialing
/// node reveals of its replica to the peer. `None` serves the full view.
///
/// The session serves entries from a store snapshot frozen at session
/// setup, so the served view is stable across the exchange; entries
/// written meanwhile travel on the next session. The snapshot is released
/// when the session ends, on every path. The whole exchange is bounded by
/// [`SYNC_SESSION_TIMEOUT`].
pub async fn connect_and_sync(
    endpoint: &Endpoint,
    sync: &SyncHandle,
    namespace: NamespaceId,
    peer: EndpointAddr,
    metrics: Option<&Metrics>,
    filter: Option<crate::filter::EntryFilter>,
) -> Result<SyncFinished, ConnectError> {
    match time::timeout(
        SYNC_SESSION_TIMEOUT,
        connect_and_sync_inner(endpoint, sync, namespace, peer, metrics, filter),
    )
    .await
    {
        Ok(res) => res,
        Err(_elapsed) => Err(ConnectError::connect(anyhow::anyhow!(
            "sync exchange timed out after {SYNC_SESSION_TIMEOUT:?}"
        ))),
    }
}

async fn connect_and_sync_inner(
    endpoint: &Endpoint,
    sync: &SyncHandle,
    namespace: NamespaceId,
    peer: EndpointAddr,
    metrics: Option<&Metrics>,
    filter: Option<crate::filter::EntryFilter>,
) -> Result<SyncFinished, ConnectError> {
    let t_start = Instant::now();
    let peer_id = peer.id;
    trace!("connect");
    let connection = endpoint
        .connect(peer, crate::ALPN)
        .await
        .map_err(ConnectError::connect)?;

    let (mut send_stream, mut recv_stream) =
        connection.open_bi().await.map_err(ConnectError::connect)?;

    let t_connect = t_start.elapsed();
    debug!(?t_connect, "connected");

    let res = run_alice(
        &mut send_stream,
        &mut recv_stream,
        sync,
        namespace,
        peer_id,
        filter,
    )
    .await;

    send_stream.finish().map_err(ConnectError::close)?;
    send_stream.stopped().await.map_err(ConnectError::close)?;
    recv_stream
        .read_to_end(0)
        .await
        .map_err(ConnectError::close)?;

    if let Some(metrics) = metrics {
        if res.is_ok() {
            metrics.sync_via_connect_success.inc();
        } else {
            metrics.sync_via_connect_failure.inc();
        }
    }

    let t_process = t_start.elapsed() - t_connect;
    match &res {
        Ok(res) => {
            debug!(
                ?t_connect,
                ?t_process,
                sent = %res.num_sent,
                recv = %res.num_recv,
                "done, ok"
            );
        }
        Err(err) => {
            debug!(?t_connect, ?t_process, ?err, "done, failed");
        }
    }

    let outcome = res?;

    let timings = Timings {
        connect: t_connect,
        process: t_process,
    };

    let res = SyncFinished {
        namespace,
        peer: peer_id,
        outcome,
        timings,
    };

    Ok(res)
}

/// Whether we want to accept or reject an incoming sync request.
#[derive(Clone)]
pub enum AcceptOutcome {
    /// Accept the sync request.
    Allow {
        /// This side's egress filter for the session: what the serving
        /// node reveals of the replica to this peer. `None` serves the
        /// full view.
        filter: Option<crate::filter::EntryFilter>,
    },
    /// Decline the sync request
    Reject(AbortReason),
}

impl std::fmt::Debug for AcceptOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptOutcome::Allow { filter: None } => write!(f, "Allow"),
            AcceptOutcome::Allow { filter: Some(_) } => write!(f, "Allow(filtered)"),
            AcceptOutcome::Reject(reason) => write!(f, "Reject({reason:?})"),
        }
    }
}

/// Handle an iroh-docs connection and sync all shared documents in the replica store.
///
/// An allowed session serves entries from a store snapshot frozen right
/// after the accept decision (see [`connect_and_sync`] for the snapshot
/// semantics); a rejected request never opens one. The whole exchange is
/// bounded by [`SYNC_SESSION_TIMEOUT`], mirroring [`connect_and_sync`]: a
/// stalled accept blocks the pair just the same.
pub async fn handle_connection<F, Fut>(
    sync: SyncHandle,
    connection: iroh::endpoint::Connection,
    accept_cb: F,
    metrics: Option<&Metrics>,
) -> Result<SyncFinished, AcceptError>
where
    F: Fn(NamespaceId, PublicKey) -> Fut,
    Fut: Future<Output = AcceptOutcome>,
{
    let peer = connection.remote_id();
    // A timeout has to name the namespace to release the pair the accept
    // decision registered as running — an error without one is routed as a
    // failure before the first message and leaves the pair running for
    // good. Named when the decision is asked for rather than when it comes
    // back, because the bound can be reached while it is still running.
    let accepted: Arc<OnceLock<NamespaceId>> = Default::default();
    let observed_accept_cb = {
        let accepted = Arc::clone(&accepted);
        move |namespace, peer| {
            let _ = accepted.set(namespace);
            accept_cb(namespace, peer)
        }
    };

    match time::timeout(
        SYNC_SESSION_TIMEOUT,
        handle_connection_inner(sync, connection, observed_accept_cb, metrics),
    )
    .await
    {
        Ok(res) => res,
        Err(_elapsed) => Err(AcceptError::sync(
            peer,
            accepted.get().copied(),
            anyhow::anyhow!("sync exchange timed out after {SYNC_SESSION_TIMEOUT:?}"),
        )),
    }
}

async fn handle_connection_inner<F, Fut>(
    sync: SyncHandle,
    connection: iroh::endpoint::Connection,
    accept_cb: F,
    metrics: Option<&Metrics>,
) -> Result<SyncFinished, AcceptError>
where
    F: Fn(NamespaceId, PublicKey) -> Fut,
    Fut: Future<Output = AcceptOutcome>,
{
    let t_start = Instant::now();
    let peer = connection.remote_id();
    let (mut send_stream, mut recv_stream) = connection
        .accept_bi()
        .await
        .map_err(|e| AcceptError::open(peer, e))?;

    let t_connect = t_start.elapsed();
    let span = error_span!("accept", peer = %peer.fmt_short(), namespace = tracing::field::Empty);
    span.in_scope(|| {
        debug!(?t_connect, "connection established");
    });

    let mut state = BobState::new(peer);
    let res = state
        .run(&mut send_stream, &mut recv_stream, sync, accept_cb)
        .instrument(span.clone())
        .await;

    if let Some(metrics) = metrics {
        if res.is_ok() {
            metrics.sync_via_accept_success.inc();
        } else {
            metrics.sync_via_accept_failure.inc();
        }
    }

    let namespace = state.namespace();
    let outcome = state.into_outcome();

    // The exchange's own result wins: teardown fails as a consequence of
    // whatever ended the exchange, so reporting the consequence buries the
    // cause the caller routes on. Nothing after the rounds may replace the
    // first error — the serving side's terminal frame swallows its own for
    // the same reason.
    let closed: Result<(), AcceptError> = async {
        send_stream
            .finish()
            .map_err(|error| AcceptError::close(peer, namespace, error))?;
        send_stream
            .stopped()
            .await
            .map_err(|error| AcceptError::close(peer, namespace, error))?;
        recv_stream
            .read_to_end(0)
            .await
            .map_err(|error| AcceptError::close(peer, namespace, error))?;
        Ok(())
    }
    .await;

    let t_process = t_start.elapsed() - t_connect;
    span.in_scope(|| match &res {
        Ok(_res) => {
            debug!(
                ?t_connect,
                ?t_process,
                sent = %outcome.num_sent,
                recv = %outcome.num_recv,
                "done, ok"
            );
        }
        Err(err) => {
            debug!(?t_connect, ?t_process, ?err, "done, failed");
        }
    });

    let namespace = res.and_then(|namespace| closed.map(|()| namespace))?;

    let timings = Timings {
        connect: t_connect,
        process: t_process,
    };
    let res = SyncFinished {
        namespace,
        outcome,
        peer,
        timings,
    };

    Ok(res)
}

/// Details of a finished sync operation.
#[derive(Debug, Clone)]
pub struct SyncFinished {
    /// The namespace that was synced.
    pub namespace: NamespaceId,
    /// The peer we syned with.
    pub peer: PublicKey,
    /// The outcome of the sync operation
    pub outcome: SyncOutcome,
    /// The time this operation took
    pub timings: Timings,
}

/// Time a sync operation took
#[derive(Debug, Default, Clone)]
pub struct Timings {
    /// Time to establish connection
    pub connect: Duration,
    /// Time to run sync exchange
    pub process: Duration,
}

/// Errors that may occur on handling incoming sync connections.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum AcceptError {
    /// Failed to establish connection
    #[error("Failed to establish connection")]
    Connect {
        #[source]
        error: anyhow::Error,
    },
    /// Failed to open replica
    #[error("Failed to open replica with {peer:?}")]
    Open {
        peer: PublicKey,
        #[source]
        error: anyhow::Error,
    },
    /// We aborted the sync request.
    #[error("Aborted sync of {namespace:?} with {peer:?}: {reason:?}")]
    Abort {
        peer: PublicKey,
        namespace: NamespaceId,
        reason: AbortReason,
    },
    /// Failed to run sync
    #[error("Failed to sync {namespace:?} with {peer:?}")]
    Sync {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        #[source]
        error: anyhow::Error,
    },
    /// Failed to close
    #[error("Failed to close {namespace:?} with {peer:?}")]
    Close {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        #[source]
        error: anyhow::Error,
    },
}

/// Errors that may occur on outgoing sync requests.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum ConnectError {
    /// Failed to establish connection
    #[error("Failed to establish connection")]
    Connect {
        #[source]
        error: anyhow::Error,
    },
    /// The remote peer aborted the sync request.
    #[error("Remote peer aborted sync: {0:?}")]
    RemoteAbort(AbortReason),
    /// Failed to run sync
    #[error("Failed to sync")]
    Sync {
        #[source]
        error: anyhow::Error,
    },
    /// Failed to close
    #[error("Failed to close connection1")]
    Close {
        #[source]
        error: anyhow::Error,
    },
}

/// Reason why we aborted an incoming sync request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AbortReason {
    /// Namespace is not available.
    NotFound,
    /// We are already syncing this namespace.
    AlreadySyncing,
    /// We experienced an error while trying to provide the requested resource
    InternalServerError,
}

impl AcceptError {
    fn open(peer: PublicKey, error: impl Into<anyhow::Error>) -> Self {
        Self::Open {
            peer,
            error: error.into(),
        }
    }
    pub(crate) fn sync(
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Sync {
            peer,
            namespace,
            error: error.into(),
        }
    }
    fn close(
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Close {
            peer,
            namespace,
            error: error.into(),
        }
    }
    /// Get the peer's node ID (if available)
    pub fn peer(&self) -> Option<PublicKey> {
        match self {
            AcceptError::Connect { .. } => None,
            AcceptError::Open { peer, .. } => Some(*peer),
            AcceptError::Sync { peer, .. } => Some(*peer),
            AcceptError::Close { peer, .. } => Some(*peer),
            AcceptError::Abort { peer, .. } => Some(*peer),
        }
    }

    /// Get the namespace (if available)
    pub fn namespace(&self) -> Option<NamespaceId> {
        match self {
            AcceptError::Connect { .. } => None,
            AcceptError::Open { .. } => None,
            AcceptError::Sync { namespace, .. } => namespace.to_owned(),
            AcceptError::Close { namespace, .. } => namespace.to_owned(),
            AcceptError::Abort { namespace, .. } => Some(*namespace),
        }
    }
}

impl ConnectError {
    fn connect(error: impl Into<anyhow::Error>) -> Self {
        Self::Connect {
            error: error.into(),
        }
    }
    fn close(error: impl Into<anyhow::Error>) -> Self {
        Self::Close {
            error: error.into(),
        }
    }
    pub(crate) fn sync(error: impl Into<anyhow::Error>) -> Self {
        Self::Sync {
            error: error.into(),
        }
    }
    pub(crate) fn remote_abort(reason: AbortReason) -> Self {
        Self::RemoteAbort(reason)
    }
}
