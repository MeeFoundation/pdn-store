//! This contains an actor spawned on a separate thread to process replica and store operations.

use std::{
    collections::{hash_map, HashMap},
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc, Weak,
    },
};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use iroh_blobs::Hash;
use irpc::channel::mpsc;
use n0_future::{
    task::JoinSet,
    time::{Duration, Instant},
    TryFutureExt,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
#[cfg(wasm_browser)]
use tracing::Instrument;
use tracing::{debug, error, error_span, trace, warn};

use crate::{
    api::{
        protocol::{AuthorListResponse, ListResponse},
        RpcError, RpcResult,
    },
    metrics::Metrics,
    ranger::Message,
    store::{
        fs::{tables::ReadOnlyTables, ContentHashesIterator, StoreInstance},
        DownloadPolicy, ImportNamespaceOutcome, Query, Store,
    },
    Author, AuthorHeads, AuthorId, Capability, CapabilityValidator, ContentStatus,
    ContentStatusCallback, Event, NamespaceId, NamespaceSecret, PeerIdBytes, RejectionObserver,
    Replica, ReplicaInfo, SignedEntry, SyncOutcome,
};

const ACTION_CAP: usize = 1024;
pub(crate) const MAX_COMMIT_DELAY: Duration = Duration::from_millis(500);

/// Hands each actor an id unique in this process, so a session id names the
/// actor that issued it and not just a position in its own counting.
static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(0);

#[derive(derive_more::Debug, derive_more::Display)]
enum Action {
    #[display("NewAuthor")]
    ImportAuthor {
        author: Author,
        #[debug("reply")]
        reply: oneshot::Sender<Result<AuthorId>>,
    },
    #[display("ExportAuthor")]
    ExportAuthor {
        author: AuthorId,
        #[debug("reply")]
        reply: oneshot::Sender<Result<Option<Author>>>,
    },
    #[display("DeleteAuthor")]
    DeleteAuthor {
        author: AuthorId,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    #[display("NewReplica")]
    ImportNamespace {
        capability: Capability,
        #[debug("reply")]
        reply: oneshot::Sender<Result<NamespaceId>>,
    },
    #[display("ListAuthors")]
    ListAuthors {
        #[debug("reply")]
        reply: mpsc::Sender<RpcResult<AuthorListResponse>>,
    },
    #[display("ListReplicas")]
    ListReplicas {
        #[debug("reply")]
        reply: mpsc::Sender<RpcResult<ListResponse>>,
    },
    #[display("ContentHashes")]
    ContentHashes {
        #[debug("reply")]
        reply: oneshot::Sender<Result<ContentHashesIterator>>,
    },
    #[display("FlushStore")]
    FlushStore {
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    #[display("Replica({}, {})", _0.fmt_short(), _1)]
    Replica(NamespaceId, ReplicaAction),
    /// Release of a session's snapshot. Top level rather than addressed to
    /// a replica: releasing needs no open replica, and a late release after
    /// the replica closed is a no-op either way.
    #[display("SyncSessionEnd")]
    SyncSessionEnd {
        session: SyncSessionId,
        /// The handle's own strong reference, riding along: the reclaim
        /// pass goes by the strong count, and a message waiting in the
        /// queue would otherwise read as a registration whose handle is
        /// gone for good.
        #[debug("alive")]
        alive: Arc<()>,
    },
    #[display("Shutdown")]
    Shutdown {
        #[debug("reply")]
        reply: Option<oneshot::Sender<Store>>,
    },
    #[cfg(test)]
    #[display("DebugTasksLen")]
    DebugTasksLen {
        #[debug("reply")]
        reply: oneshot::Sender<usize>,
    },
    #[cfg(test)]
    #[display("DebugSessionCount")]
    DebugSessionCount {
        #[debug("reply")]
        reply: oneshot::Sender<usize>,
    },
}

#[derive(derive_more::Debug, strum::Display)]
enum ReplicaAction {
    Open {
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
        opts: OpenOpts,
    },
    Close {
        #[debug("reply")]
        reply: oneshot::Sender<Result<bool>>,
    },
    GetState {
        #[debug("reply")]
        reply: oneshot::Sender<Result<OpenState>>,
    },
    SetSync {
        sync: bool,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        sender: async_channel::Sender<Event>,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        sender: async_channel::Sender<Event>,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    InsertLocal {
        author: AuthorId,
        key: Bytes,
        hash: Hash,
        len: u64,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    DeletePrefix {
        author: AuthorId,
        key: Bytes,
        #[debug("reply")]
        reply: oneshot::Sender<Result<usize>>,
    },
    RetractEntry {
        author: AuthorId,
        key: Bytes,
        up_to_timestamp: u64,
        #[debug("reply")]
        reply: oneshot::Sender<Result<bool>>,
    },
    InsertRemote {
        entry: SignedEntry,
        from: PeerIdBytes,
        content_status: ContentStatus,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    SyncSessionStart {
        #[debug("reply")]
        reply: oneshot::Sender<Result<(SyncSessionId, Arc<()>)>>,
    },
    SyncInitialMessage {
        session: SyncSessionId,
        #[debug("filter")]
        filter: Option<crate::filter::EntryFilter>,
        #[debug("reply")]
        reply: oneshot::Sender<Result<Message<SignedEntry>>>,
    },
    SyncProcessMessage {
        message: Message<SignedEntry>,
        from: PeerIdBytes,
        state: SyncOutcome,
        session: SyncSessionId,
        #[debug("filter")]
        filter: Option<crate::filter::EntryFilter>,
        #[debug("reply")]
        reply: oneshot::Sender<Result<(Option<Message<SignedEntry>>, SyncOutcome)>>,
    },
    GetSyncPeers {
        #[debug("reply")]
        reply: oneshot::Sender<Result<Option<Vec<PeerIdBytes>>>>,
    },
    RegisterUsefulPeer {
        peer: PeerIdBytes,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    GetExact {
        author: AuthorId,
        key: Bytes,
        include_empty: bool,
        reply: oneshot::Sender<Result<Option<SignedEntry>>>,
    },
    GetMany {
        query: Query,
        reply: mpsc::Sender<RpcResult<SignedEntry>>,
    },
    DropReplica {
        reply: oneshot::Sender<Result<()>>,
    },
    ExportSecretKey {
        reply: oneshot::Sender<Result<NamespaceSecret>>,
    },
    HasNewsForUs {
        heads: AuthorHeads,
        #[debug("reply")]
        reply: oneshot::Sender<Result<Option<NonZeroU64>>>,
    },
    SetDownloadPolicy {
        policy: DownloadPolicy,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    GetDownloadPolicy {
        #[debug("reply")]
        reply: oneshot::Sender<Result<DownloadPolicy>>,
    },
}

/// The state for an open replica.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenState {
    /// Whether to accept sync requests for this replica.
    pub sync: bool,
    /// How many event subscriptions are open
    pub subscribers: usize,
    /// By how many handles the replica is currently held open
    pub handles: usize,
}

#[derive(Debug)]
struct OpenReplica {
    info: ReplicaInfo,
    sync: bool,
    handles: usize,
}

/// Identifies one sync session's frozen read snapshot inside the actor.
///
/// The issuing actor is part of the identity. Each actor counts its own
/// sessions from zero, so without it the first session of one handle names
/// the first session of another, and a handle handed a foreign id would
/// resolve its own snapshot of that namespace and serve a session from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SyncSessionId {
    actor: u64,
    seq: u64,
}

/// Handle to one sync session's read snapshot in the actor.
///
/// The snapshot pins a redb read transaction, so its lifetime must stay
/// within session bounds: dropping the handle releases it, which covers
/// every session exit path — success, error, and cancellation alike.
///
/// The handle's liveness, not the release message, is what the actor goes
/// by, and the strong references to a registration are this handle plus a
/// release message of its own still in the queue. An entry therefore
/// outlives both by no more than the actor's next tick, whether the message
/// was lost to a full queue or the handle was never built at all — a caller
/// cancelled between registration and reply leaves the reference in the
/// undelivered reply. The message is the prompt path; counting a queued one
/// as lost would make the reclaim metric fire on ordinary exchanges.
#[must_use = "dropping the handle ends the session and releases its snapshot"]
#[derive(Debug)]
pub struct SyncSession {
    id: SyncSessionId,
    namespace: NamespaceId,
    tx: async_channel::Sender<Action>,
    /// Held, never read: the actor watches the strong count.
    _alive: Arc<()>,
}

impl SyncSession {
    /// The id to pass into [`SyncHandle::sync_initial_message`] and
    /// [`SyncHandle::sync_process_message`].
    pub fn id(&self) -> SyncSessionId {
        self.id
    }

    /// The namespace this session is of.
    pub fn namespace(&self) -> NamespaceId {
        self.namespace
    }
}

impl Drop for SyncSession {
    fn drop(&mut self) {
        let _ = self.tx.try_send(Action::SyncSessionEnd {
            session: self.id,
            // Cloning here is sound because a `Drop` body runs before
            // the fields it drops: this is a second strong reference,
            // not the last one resurrected.
            alive: Arc::clone(&self._alive),
        });
    }
}

/// A session's frozen snapshot, held by the actor until released.
#[derive(derive_more::Debug)]
struct SessionSnapshot {
    namespace: NamespaceId,
    #[debug("ReadOnlyTables")]
    tables: ReadOnlyTables,
    /// Weak counterpart of the strong references — the handle, and a
    /// release message of its own still in the queue. Once it holds
    /// nothing, neither exists, no handle can name this snapshot again,
    /// and the actor reclaims it on its next tick.
    alive: Weak<()>,
}

/// The [`SyncHandle`] controls an actor thread which executes replica and store operations.
///
/// The [`SyncHandle`] exposes async methods which all send messages into the actor thread, usually
/// returning something via a return channel. The actor thread itself is a regular [`std::thread`]
/// which processes incoming messages sequentially.
///
/// The handle is cheaply cloneable. Once the last clone is dropped, the actor thread is joined.
/// The thread will finish processing all messages in the channel queue, and then exit.
/// To prevent this last drop from blocking the calling thread, you can call [`SyncHandle::shutdown`]
/// and await its result before dropping the last [`SyncHandle`]. This ensures that
/// waiting for the actor to finish happens in an async context, and therefore that the final
/// [`SyncHandle::drop`] will not block.
#[derive(Debug, Clone)]
pub struct SyncHandle {
    tx: async_channel::Sender<Action>,
    #[cfg(wasm_browser)]
    #[allow(unused)]
    join_handle: Arc<Option<n0_future::task::JoinHandle<()>>>,
    #[cfg(not(wasm_browser))]
    join_handle: Arc<Option<std::thread::JoinHandle<()>>>,
    metrics: Arc<Metrics>,
}

/// Options when opening a replica.
#[derive(Debug, Default)]
pub struct OpenOpts {
    /// Set to true to set sync state to true.
    pub sync: bool,
    /// Optionally subscribe to replica events.
    pub subscribe: Option<async_channel::Sender<Event>>,
}

impl OpenOpts {
    /// Set sync state to true.
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }
    /// Subscribe to replica events.
    pub fn subscribe(mut self, subscribe: async_channel::Sender<Event>) -> Self {
        self.subscribe = Some(subscribe);
        self
    }
}

#[allow(missing_docs)]
impl SyncHandle {
    /// Spawn a sync actor and return a handle.
    pub fn spawn(
        store: Store,
        content_status_callback: Option<ContentStatusCallback>,
        capability_validator: Option<CapabilityValidator>,
        rejection_observer: Option<RejectionObserver>,
        me: String,
    ) -> SyncHandle {
        let metrics = Arc::new(Metrics::default());
        let (action_tx, action_rx) = async_channel::bounded(ACTION_CAP);
        let actor = Actor {
            actor_id: NEXT_ACTOR_ID.fetch_add(1, AtomicOrdering::Relaxed),
            store,
            states: Default::default(),
            sessions: Default::default(),
            next_session_id: 0,
            last_session_sweep: Instant::now(),
            action_rx,
            content_status_callback,
            capability_validator,
            rejection_observer,
            tasks: Default::default(),
            metrics: metrics.clone(),
        };

        let span = error_span!("sync", %me);
        #[cfg(wasm_browser)]
        let join_handle = n0_future::task::spawn(actor.run_async().instrument(span));

        #[cfg(not(wasm_browser))]
        let join_handle = std::thread::Builder::new()
            .name("sync-actor".to_string())
            .spawn(move || {
                let _enter = span.enter();

                if let Err(err) = actor.run_in_thread() {
                    error!("Sync actor closed with error: {err:?}");
                }
            })
            .expect("failed to spawn thread");

        let join_handle = Arc::new(Some(join_handle));
        SyncHandle {
            tx: action_tx,
            join_handle,
            metrics,
        }
    }

    /// Returns the metrics collected in this sync actor.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    pub async fn open(&self, namespace: NamespaceId, opts: OpenOpts) -> Result<()> {
        tracing::debug!("SyncHandle::open called");
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::Open { reply, opts };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn close(&self, namespace: NamespaceId) -> Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.send_replica(namespace, ReplicaAction::Close { reply })
            .await?;
        rx.await?
    }

    pub async fn subscribe(
        &self,
        namespace: NamespaceId,
        sender: async_channel::Sender<Event>,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send_replica(namespace, ReplicaAction::Subscribe { sender, reply })
            .await?;
        rx.await?
    }

    pub async fn unsubscribe(
        &self,
        namespace: NamespaceId,
        sender: async_channel::Sender<Event>,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send_replica(namespace, ReplicaAction::Unsubscribe { sender, reply })
            .await?;
        rx.await?
    }

    pub async fn set_sync(&self, namespace: NamespaceId, sync: bool) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SetSync { sync, reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn insert_local(
        &self,
        namespace: NamespaceId,
        author: AuthorId,
        key: Bytes,
        hash: Hash,
        len: u64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::InsertLocal {
            author,
            key,
            hash,
            len,
            reply,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn delete_prefix(
        &self,
        namespace: NamespaceId,
        author: AuthorId,
        key: Bytes,
    ) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::DeletePrefix { author, key, reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn insert_remote(
        &self,
        namespace: NamespaceId,
        entry: SignedEntry,
        from: PeerIdBytes,
        content_status: ContentStatus,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::InsertRemote {
            entry,
            from,
            content_status,
            reply,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    /// Open a sync session on `namespace`: freeze a read snapshot for the
    /// session's egress.
    ///
    /// Every read the session serves the peer derives from the snapshot,
    /// so the served view is stable across the session's rounds while
    /// writes continue on the live store. Opening commits the store's
    /// pending write batch, so the snapshot holds every entry inserted
    /// before session setup. The returned handle owns the snapshot;
    /// dropping it releases the snapshot on any session exit path.
    pub async fn sync_session_start(&self, namespace: NamespaceId) -> Result<SyncSession> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SyncSessionStart { reply };
        self.send_replica(namespace, action).await?;
        let (id, alive) = rx.await??;
        Ok(SyncSession {
            id,
            namespace,
            tx: self.tx.clone(),
            _alive: alive,
        })
    }

    /// Register a session and abandon the reply, as a caller cancelled
    /// between the two does.
    #[cfg(test)]
    pub(crate) async fn debug_abandon_session_start(&self, namespace: NamespaceId) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SyncSessionStart { reply };
        self.send_replica(namespace, action).await?;
        drop(rx);
        Ok(())
    }

    pub async fn sync_initial_message(
        &self,
        namespace: NamespaceId,
        session: SyncSessionId,
        filter: Option<crate::filter::EntryFilter>,
    ) -> Result<Message<SignedEntry>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SyncInitialMessage {
            session,
            filter,
            reply,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn sync_process_message(
        &self,
        namespace: NamespaceId,
        message: Message<SignedEntry>,
        from: PeerIdBytes,
        state: SyncOutcome,
        session: SyncSessionId,
        filter: Option<crate::filter::EntryFilter>,
    ) -> Result<(Option<Message<SignedEntry>>, SyncOutcome)> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SyncProcessMessage {
            reply,
            message,
            from,
            state,
            session,
            filter,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    /// Number of registered sync-session snapshots (test observability).
    #[cfg(test)]
    pub(crate) async fn debug_session_count(&self) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::DebugSessionCount { reply }).await?;
        Ok(rx.await?)
    }

    pub async fn get_sync_peers(&self, namespace: NamespaceId) -> Result<Option<Vec<PeerIdBytes>>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetSyncPeers { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn register_useful_peer(
        &self,
        namespace: NamespaceId,
        peer: PeerIdBytes,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::RegisterUsefulPeer { reply, peer };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn has_news_for_us(
        &self,
        namespace: NamespaceId,
        heads: AuthorHeads,
    ) -> Result<Option<NonZeroU64>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::HasNewsForUs { reply, heads };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn get_many(
        &self,
        namespace: NamespaceId,
        query: Query,
        reply: mpsc::Sender<RpcResult<SignedEntry>>,
    ) -> Result<()> {
        let action = ReplicaAction::GetMany { query, reply };
        self.send_replica(namespace, action).await?;
        Ok(())
    }

    pub async fn get_exact(
        &self,
        namespace: NamespaceId,
        author: AuthorId,
        key: Bytes,
        include_empty: bool,
    ) -> Result<Option<SignedEntry>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetExact {
            author,
            key,
            include_empty,
            reply,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    /// Physically remove the record of `author` at `key`, if its timestamp
    /// is at or below `up_to_timestamp` (see
    /// [`Store::retract_entry`](crate::store::Store::retract_entry)).
    pub async fn retract_entry(
        &self,
        namespace: NamespaceId,
        author: AuthorId,
        key: Bytes,
        up_to_timestamp: u64,
    ) -> Result<bool> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::RetractEntry {
            author,
            key,
            up_to_timestamp,
            reply,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn drop_replica(&self, namespace: NamespaceId) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::DropReplica { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn export_secret_key(&self, namespace: NamespaceId) -> Result<NamespaceSecret> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::ExportSecretKey { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn get_state(&self, namespace: NamespaceId) -> Result<OpenState> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetState { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn shutdown(&self) -> Result<Store> {
        let (reply, rx) = oneshot::channel();
        let action = Action::Shutdown { reply: Some(reply) };
        self.send(action).await?;
        let store = rx.await?;
        Ok(store)
    }

    #[cfg(test)]
    async fn debug_tasks_len(&self) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::DebugTasksLen { reply }).await?;
        Ok(rx.await?)
    }

    pub async fn list_authors(
        &self,
        reply: mpsc::Sender<RpcResult<AuthorListResponse>>,
    ) -> Result<()> {
        self.send(Action::ListAuthors { reply }).await
    }

    pub async fn list_replicas(&self, reply: mpsc::Sender<RpcResult<ListResponse>>) -> Result<()> {
        self.send(Action::ListReplicas { reply }).await
    }

    pub async fn import_author(&self, author: Author) -> Result<AuthorId> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::ImportAuthor { author, reply }).await?;
        rx.await?
    }

    pub async fn export_author(&self, author: AuthorId) -> Result<Option<Author>> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::ExportAuthor { author, reply }).await?;
        rx.await?
    }

    pub async fn delete_author(&self, author: AuthorId) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::DeleteAuthor { author, reply }).await?;
        rx.await?
    }

    pub async fn import_namespace(&self, capability: Capability) -> Result<NamespaceId> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::ImportNamespace { capability, reply })
            .await?;
        rx.await?
    }

    pub async fn get_download_policy(&self, namespace: NamespaceId) -> Result<DownloadPolicy> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetDownloadPolicy { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn set_download_policy(
        &self,
        namespace: NamespaceId,
        policy: DownloadPolicy,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SetDownloadPolicy { reply, policy };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn content_hashes(&self) -> Result<ContentHashesIterator> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::ContentHashes { reply }).await?;
        rx.await?
    }

    /// Makes sure that all pending database operations are persisted to disk.
    ///
    /// Otherwise, database operations are batched into bigger transactions for speed.
    /// Use this if you need to make sure something is written to the database
    /// before another operation, e.g. to make sure sudden process exits don't corrupt
    /// your application state.
    ///
    /// It's not necessary to call this function before shutdown, as `shutdown` will
    /// trigger a flush on its own.
    pub async fn flush_store(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::FlushStore { reply }).await?;
        rx.await?
    }

    async fn send(&self, action: Action) -> Result<()> {
        self.tx
            .send(action)
            .await
            .context("sending to iroh_docs actor failed")?;
        Ok(())
    }
    async fn send_replica(&self, namespace: NamespaceId, action: ReplicaAction) -> Result<()> {
        self.send(Action::Replica(namespace, action)).await?;
        Ok(())
    }
}

impl Drop for SyncHandle {
    fn drop(&mut self) {
        // this means we're dropping the last reference
        #[allow(unused)]
        if let Some(handle) = Arc::get_mut(&mut self.join_handle) {
            #[cfg(wasm_browser)]
            {
                let tx = self.tx.clone();
                n0_future::task::spawn(async move {
                    tx.send(Action::Shutdown { reply: None }).await.ok();
                });
            }
            #[cfg(not(wasm_browser))]
            {
                // this call is the reason tx can not be a tokio mpsc channel.
                // we have no control about where drop is called, yet tokio send_blocking panics
                // when called from inside a tokio runtime.
                self.tx.send_blocking(Action::Shutdown { reply: None }).ok();
                let handle = handle.take().expect("this can only run once");

                if let Err(err) = handle.join() {
                    warn!(?err, "Failed to join sync actor");
                }
            }
        }
    }
}

struct Actor {
    /// This actor's id in the process; part of every session id it issues.
    actor_id: u64,
    store: Store,
    states: OpenReplicas,
    /// Frozen read snapshots of running sync sessions, keyed by session id.
    ///
    /// Each entry pins a redb read transaction until removed: the session
    /// guard's drop removes it, and closing the replica sweeps whatever a
    /// lost release message left behind.
    sessions: HashMap<SyncSessionId, SessionSnapshot>,
    next_session_id: u64,
    last_session_sweep: Instant,
    action_rx: async_channel::Receiver<Action>,
    content_status_callback: Option<ContentStatusCallback>,
    capability_validator: Option<CapabilityValidator>,
    rejection_observer: Option<RejectionObserver>,
    tasks: JoinSet<()>,
    metrics: Arc<Metrics>,
}

impl Actor {
    #[cfg(not(wasm_browser))]
    fn run_in_thread(self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let local_set = tokio::task::LocalSet::new();
        local_set.block_on(&rt, async move { self.run_async().await });
        Ok(())
    }

    async fn run_async(mut self) {
        let reply = loop {
            let timeout = n0_future::time::sleep(MAX_COMMIT_DELAY);
            tokio::pin!(timeout);
            let action = tokio::select! {
                _ = &mut timeout => {
                    // Before the flush, not after: releasing the read
                    // transactions of sessions whose handle is gone lets
                    // this very commit reclaim the pages they pinned.
                    self.reclaim_abandoned_sessions();
                    if let Err(cause) = self.store.flush() {
                        error!(?cause, "failed to flush store");
                    }
                    continue;
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Err(err) = res {
                        if !err.is_cancelled() {
                            warn!(?err, "actor reply-streamer task panicked");
                        }
                    }
                    continue;
                }
                action = self.action_rx.recv() => {
                    match action {
                        Ok(action) => action,
                        Err(async_channel::RecvError) => {
                            debug!("action channel disconnected");
                            break None;
                        }

                    }
                }
            };
            trace!(%action, "tick");
            self.metrics.actor_tick_main.inc();
            // The arm above runs only while the actor is idle, and an actor
            // under load is exactly where a pinned page set hurts.
            self.reclaim_abandoned_sessions();
            match action {
                Action::Shutdown { reply } => {
                    break reply;
                }
                action => {
                    if self.on_action(action).await.is_err() {
                        warn!("failed to send reply: receiver dropped");
                    }
                }
            }
        };

        if let Err(cause) = self.store.flush() {
            warn!(?cause, "failed to flush store");
        }
        self.close_all();
        self.tasks.abort_all();
        debug!("docs actor shutdown");
        if let Some(reply) = reply {
            reply.send(self.store).ok();
        }
    }

    async fn on_action(&mut self, action: Action) -> Result<(), SendReplyError> {
        match action {
            Action::Shutdown { .. } => {
                unreachable!("Shutdown is handled in run()")
            }
            #[cfg(test)]
            Action::DebugTasksLen { reply } => send_reply(reply, self.tasks.len()),
            #[cfg(test)]
            Action::DebugSessionCount { reply } => send_reply(reply, self.sessions.len()),
            Action::ImportAuthor { author, reply } => {
                let id = author.id();
                send_reply(reply, self.store.import_author(author).map(|_| id))
            }
            Action::ExportAuthor { author, reply } => {
                send_reply(reply, self.store.get_author(&author))
            }
            Action::DeleteAuthor { author, reply } => {
                send_reply(reply, self.store.delete_author(author))
            }
            Action::ImportNamespace { capability, reply } => send_reply_with(reply, self, |this| {
                let id = capability.id();
                let outcome = this.store.import_namespace(capability.clone())?;
                if let ImportNamespaceOutcome::Upgraded = outcome {
                    if let Ok(state) = this.states.get_mut(&id) {
                        state.info.merge_capability(capability)?;
                    }
                }
                Ok(id)
            }),
            Action::ListAuthors { reply } => {
                let iter = self
                    .store
                    .list_authors()
                    .map(|a| a.map(|a| a.map(|a| AuthorListResponse { author_id: a.id() })));
                self.tasks.spawn_local(async move {
                    iter_to_irpc(reply, iter).await.ok();
                });
                Ok(())
            }
            Action::ListReplicas { reply } => {
                let iter = self.store.list_namespaces();
                let iter = iter.map(|inner| {
                    inner.map(|res| res.map(|(id, capability)| ListResponse { id, capability }))
                });
                self.tasks.spawn_local(async move {
                    iter_to_irpc(reply, iter).await.ok();
                });
                Ok(())
            }
            Action::ContentHashes { reply } => {
                send_reply_with(reply, self, |this| this.store.content_hashes())
            }
            Action::FlushStore { reply } => send_reply(reply, self.store.flush()),
            Action::SyncSessionEnd { session, alive } => {
                self.sessions.remove(&session);
                // Kept the registration out of the reclaim pass while this
                // message queued; with the entry gone its work is done.
                drop(alive);
                self.record_open_sessions();
                Ok(())
            }
            Action::Replica(namespace, action) => self.on_replica_action(namespace, action).await,
        }
    }

    /// The replica a session's exchange runs against: egress reads through
    /// the snapshot that session registered.
    ///
    /// Resolving the session and handing over its snapshot are one step on
    /// purpose: two call sites are two places to pass `None` and serve the
    /// peer live reads instead, and the store answers either way, so only a
    /// test that runs an exchange tells the two apart.
    fn session_replica(
        &mut self,
        namespace: &NamespaceId,
        session: SyncSessionId,
    ) -> Result<Replica<'_, &mut ReplicaInfo>> {
        let snapshot = session_snapshot(&self.sessions, self.actor_id, session, namespace)?;
        self.states
            .replica_if_syncing(namespace, &mut self.store, Some(snapshot))
    }

    async fn on_replica_action(
        &mut self,
        namespace: NamespaceId,
        action: ReplicaAction,
    ) -> Result<(), SendReplyError> {
        match action {
            ReplicaAction::Open { reply, opts } => {
                tracing::trace!("open in");
                let res = self.open(namespace, opts);
                tracing::trace!("open out");
                send_reply(reply, res)
            }
            ReplicaAction::Close { reply } => {
                let res = self.close(namespace);
                // ignore errors when no receiver is present for close
                reply.send(Ok(res)).ok();
                Ok(())
            }
            ReplicaAction::Subscribe { sender, reply } => send_reply_with(reply, self, |this| {
                let state = this.states.get_mut(&namespace)?;
                state.info.subscribe(sender);
                Ok(())
            }),
            ReplicaAction::Unsubscribe { sender, reply } => send_reply_with(reply, self, |this| {
                let state = this.states.get_mut(&namespace)?;
                state.info.unsubscribe(&sender);
                drop(sender);
                Ok(())
            }),
            ReplicaAction::SetSync { sync, reply } => send_reply_with(reply, self, |this| {
                let state = this.states.get_mut(&namespace)?;
                state.sync = sync;
                Ok(())
            }),
            ReplicaAction::InsertLocal {
                author,
                key,
                hash,
                len,
                reply,
            } => {
                send_reply_with_async(reply, self, async move |this| {
                    let author = get_author(&mut this.store, &author)?;
                    let mut replica = this.states.replica(namespace, &mut this.store)?;
                    replica.insert(&key, &author, hash, len).await?;
                    this.metrics.new_entries_local.inc();
                    this.metrics.new_entries_local_size.inc_by(len);
                    Ok(())
                })
                .await
            }
            ReplicaAction::DeletePrefix { author, key, reply } => {
                send_reply_with_async(reply, self, async |this| {
                    let author = get_author(&mut this.store, &author)?;
                    let mut replica = this.states.replica(namespace, &mut this.store)?;
                    let res = replica.delete_prefix(&key, &author).await?;
                    Ok(res)
                })
                .await
            }
            ReplicaAction::InsertRemote {
                entry,
                from,
                content_status,
                reply,
            } => {
                send_reply_with_async(reply, self, async move |this| {
                    // Ingest reads live: an entry the peer sends is judged
                    // against current state, never a session's frozen view.
                    let mut replica =
                        this.states
                            .replica_if_syncing(&namespace, &mut this.store, None)?;
                    let len = entry.content_len();
                    replica
                        .insert_remote_entry(entry, from, content_status)
                        .await?;
                    this.metrics.new_entries_remote.inc();
                    this.metrics.new_entries_remote_size.inc_by(len);
                    Ok(())
                })
                .await
            }

            ReplicaAction::SyncSessionStart { reply } => send_reply_with(reply, self, |this| {
                this.states.ensure_syncing(&namespace)?;
                // Committing the pending write batch is part of the
                // contract (`snapshot_owned` flushes): the snapshot holds
                // every entry inserted before session setup.
                let tables = this.store.snapshot_owned()?;
                let id = this.next_session_id;
                this.next_session_id += 1;
                // The strong reference travels in the reply, so an
                // undelivered one leaves the registration unreferenced and
                // the tick reclaims it.
                let alive = Arc::new(());
                let id = SyncSessionId {
                    actor: this.actor_id,
                    seq: id,
                };
                this.sessions.insert(
                    id,
                    SessionSnapshot {
                        namespace,
                        tables,
                        alive: Arc::downgrade(&alive),
                    },
                );
                this.record_open_sessions();
                Ok((id, alive))
            }),
            ReplicaAction::SyncInitialMessage {
                session,
                filter,
                reply,
            } => send_reply_with(reply, self, move |this| {
                let res = this
                    .session_replica(&namespace, session)?
                    .sync_initial_message(filter)?;
                Ok(res)
            }),
            ReplicaAction::SyncProcessMessage {
                message,
                from,
                mut state,
                session,
                filter,
                reply,
            } => {
                let res = async {
                    let mut replica = self.session_replica(&namespace, session)?;
                    let res = replica
                        .sync_process_message(message, from, &mut state, filter)
                        .await?;
                    Ok((res, state))
                }
                .await;
                reply.send(res).map_err(send_reply_error)
            }
            ReplicaAction::GetSyncPeers { reply } => send_reply_with(reply, self, move |this| {
                this.states.ensure_open(&namespace)?;
                let peers = this.store.get_sync_peers(&namespace)?;
                Ok(peers.map(|iter| iter.collect()))
            }),
            ReplicaAction::RegisterUsefulPeer { peer, reply } => {
                let res = self.store.register_useful_peer(namespace, peer);
                send_reply(reply, res)
            }
            ReplicaAction::GetExact {
                author,
                key,
                include_empty,
                reply,
            } => send_reply_with(reply, self, move |this| {
                this.states.ensure_open(&namespace)?;
                this.store.get_exact(namespace, author, key, include_empty)
            }),
            ReplicaAction::RetractEntry {
                author,
                key,
                up_to_timestamp,
                reply,
            } => send_reply_with(reply, self, move |this| {
                this.states.ensure_open(&namespace)?;
                this.store
                    .retract_entry(namespace, author, &key, up_to_timestamp)
            }),
            ReplicaAction::GetMany { query, reply } => {
                let iter = self
                    .states
                    .ensure_open(&namespace)
                    .and_then(|_| self.store.get_many(namespace, query));
                self.tasks
                    .spawn_local(iter_to_irpc(reply, iter).map_ok_or_else(|_| (), |_| ()));
                Ok(())
            }
            ReplicaAction::DropReplica { reply } => send_reply_with(reply, self, |this| {
                this.close(namespace);
                this.store.remove_replica(&namespace)
            }),
            ReplicaAction::ExportSecretKey { reply } => {
                let res = self
                    .states
                    .get_mut(&namespace)
                    .and_then(|state| Ok(state.info.capability.secret_key()?.clone()));
                send_reply(reply, res)
            }
            ReplicaAction::GetState { reply } => send_reply_with(reply, self, move |this| {
                let state = this.states.get_mut(&namespace)?;
                let handles = state.handles;
                let sync = state.sync;
                let subscribers = state.info.subscribers_count();
                Ok(OpenState {
                    handles,
                    sync,
                    subscribers,
                })
            }),
            ReplicaAction::HasNewsForUs { heads, reply } => {
                let res = self.store.has_news_for_us(namespace, &heads);
                send_reply(reply, res)
            }
            ReplicaAction::SetDownloadPolicy { policy, reply } => {
                send_reply(reply, self.store.set_download_policy(&namespace, policy))
            }
            ReplicaAction::GetDownloadPolicy { reply } => {
                send_reply(reply, self.store.get_download_policy(&namespace))
            }
        }
    }

    /// Drop the snapshots of sessions whose handle is gone, no more often
    /// than the actor's own housekeeping cadence.
    ///
    /// A handle releases its snapshot by message; this covers the cases
    /// where no message comes — a release lost to a full queue, and a
    /// registration whose handle a cancelled caller never received.
    fn reclaim_abandoned_sessions(&mut self) {
        if self.sessions.is_empty() {
            return;
        }
        if self.last_session_sweep.elapsed() < MAX_COMMIT_DELAY {
            return;
        }
        self.last_session_sweep = Instant::now();
        let before = self.sessions.len();
        self.sessions.retain(|_, s| s.alive.strong_count() > 0);
        let reclaimed = before - self.sessions.len();
        if reclaimed > 0 {
            self.metrics
                .sync_sessions_reclaimed
                .inc_by(reclaimed as u64);
        }
        self.record_open_sessions();
    }

    /// Publish how many snapshots are held, from the map rather than by
    /// counting the sites that change it, so the two cannot drift apart.
    fn record_open_sessions(&self) {
        self.metrics
            .sync_sessions_open
            .set(self.sessions.len() as i64);
    }

    fn close(&mut self, namespace: NamespaceId) -> bool {
        let res = self.states.close(namespace);
        if res {
            // A closed replica has no sessions; this also reclaims
            // snapshots whose release message was lost.
            self.sessions.retain(|_, s| s.namespace != namespace);
            self.record_open_sessions();
            self.store.close_replica(namespace);
        }
        res
    }

    fn close_all(&mut self) {
        self.sessions.clear();
        self.record_open_sessions();
        for id in self.states.close_all() {
            self.store.close_replica(id);
        }
    }

    fn open(&mut self, namespace: NamespaceId, opts: OpenOpts) -> Result<()> {
        let open_cb = || {
            let mut info = self.store.load_replica_info(&namespace)?;
            if let Some(cb) = &self.content_status_callback {
                info.set_content_status_callback(Arc::clone(cb));
            }
            if let Some(validator) = &self.capability_validator {
                info.set_capability_validator(Arc::clone(validator));
            }
            if let Some(observer) = &self.rejection_observer {
                info.set_rejection_observer(Arc::clone(observer));
            }
            Ok(info)
        };
        self.states.open_with(namespace, opts, open_cb)
    }
}

/// Resolve a session id to its frozen snapshot.
///
/// The id must name a registered session of this namespace, issued by this
/// actor: anything else is an error, so a session whose snapshot is gone
/// (the replica was closed under it) or whose id came from another handle
/// fails instead of silently serving a different view.
fn session_snapshot<'a>(
    sessions: &'a HashMap<SyncSessionId, SessionSnapshot>,
    actor_id: u64,
    session: SyncSessionId,
    namespace: &NamespaceId,
) -> Result<&'a ReadOnlyTables> {
    anyhow::ensure!(
        session.actor == actor_id,
        "sync session was issued by another actor"
    );
    let entry = sessions
        .get(&session)
        .context("sync session not registered")?;
    anyhow::ensure!(
        entry.namespace == *namespace,
        "sync session belongs to another namespace"
    );
    Ok(&entry.tables)
}

#[derive(Default)]
struct OpenReplicas(HashMap<NamespaceId, OpenReplica>);

impl OpenReplicas {
    fn replica<'a, 'b>(
        &'a mut self,
        namespace: NamespaceId,
        store: &'b mut Store,
    ) -> Result<Replica<'b, &'a mut ReplicaInfo>> {
        let state = self.get_mut(&namespace)?;
        Ok(Replica::new(
            StoreInstance::new(state.info.capability.id(), store),
            &mut state.info,
        ))
    }

    /// The replica to run a sync exchange against, reading through
    /// `session_snapshot`.
    ///
    /// The snapshot is a parameter rather than something assigned onto the
    /// replica afterwards: a caller cannot then forget it and silently
    /// serve live reads, which is the drift this whole mechanism exists to
    /// prevent. Ingest passes `None` and says so at the call site.
    fn replica_if_syncing<'a, 'b>(
        &'a mut self,
        namespace: &NamespaceId,
        store: &'b mut Store,
        session_snapshot: Option<&'b ReadOnlyTables>,
    ) -> Result<Replica<'b, &'a mut ReplicaInfo>> {
        self.ensure_syncing(namespace)?;
        let state = self.get_mut(namespace)?;
        Ok(Replica::new(
            StoreInstance::with_session_snapshot(
                state.info.capability.id(),
                store,
                session_snapshot,
            ),
            &mut state.info,
        ))
    }

    fn ensure_syncing(&mut self, namespace: &NamespaceId) -> Result<()> {
        let state = self.get_mut(namespace)?;
        anyhow::ensure!(state.sync, "sync is not enabled for replica");
        Ok(())
    }

    fn get_mut(&mut self, namespace: &NamespaceId) -> Result<&mut OpenReplica> {
        self.0.get_mut(namespace).context("replica not open")
    }

    fn is_open(&self, namespace: &NamespaceId) -> bool {
        self.0.contains_key(namespace)
    }

    fn ensure_open(&self, namespace: &NamespaceId) -> Result<()> {
        match self.is_open(namespace) {
            true => Ok(()),
            false => Err(anyhow!("replica not open")),
        }
    }
    fn open_with(
        &mut self,
        namespace: NamespaceId,
        opts: OpenOpts,
        mut open_cb: impl FnMut() -> Result<ReplicaInfo>,
    ) -> Result<()> {
        match self.0.entry(namespace) {
            hash_map::Entry::Vacant(e) => {
                let mut info = open_cb()?;
                if let Some(sender) = opts.subscribe {
                    info.subscribe(sender);
                }
                debug!(namespace = %namespace.fmt_short(), "open");
                let state = OpenReplica {
                    info,
                    sync: opts.sync,
                    handles: 1,
                };
                e.insert(state);
            }
            hash_map::Entry::Occupied(mut e) => {
                let state = e.get_mut();
                state.handles += 1;
                state.sync = state.sync || opts.sync;
                if let Some(sender) = opts.subscribe {
                    state.info.subscribe(sender);
                }
            }
        }
        Ok(())
    }
    fn close(&mut self, namespace: NamespaceId) -> bool {
        match self.0.entry(namespace) {
            hash_map::Entry::Vacant(_e) => {
                warn!(namespace = %namespace.fmt_short(), "received close request for closed replica");
                true
            }
            hash_map::Entry::Occupied(mut e) => {
                let state = e.get_mut();
                tracing::debug!("STATE {state:?}");
                state.handles = state.handles.wrapping_sub(1);
                if state.handles == 0 {
                    let _ = e.remove_entry();
                    debug!(namespace = %namespace.fmt_short(), "close");
                    true
                } else {
                    false
                }
            }
        }
    }

    fn close_all(&mut self) -> impl Iterator<Item = NamespaceId> + '_ {
        self.0.drain().map(|(n, _s)| n)
    }
}

async fn iter_to_irpc<T: irpc::RpcMessage>(
    channel: mpsc::Sender<RpcResult<T>>,
    iter: Result<impl Iterator<Item = Result<T>>>,
) -> Result<(), SendReplyError> {
    match iter {
        Err(err) => channel
            .send(Err(RpcError::new(&*err)))
            .await
            .map_err(send_reply_error)?,
        Ok(iter) => {
            for item in iter {
                let item = item.map_err(|err| RpcError::new(&*err));
                channel.send(item).await.map_err(send_reply_error)?;
            }
        }
    }
    Ok(())
}

fn get_author(store: &mut Store, id: &AuthorId) -> Result<Author> {
    store.get_author(id)?.context("author not found")
}

#[derive(Debug)]
struct SendReplyError;

fn send_reply<T>(sender: oneshot::Sender<T>, value: T) -> Result<(), SendReplyError> {
    sender.send(value).map_err(send_reply_error)
}

fn send_reply_with<T>(
    sender: oneshot::Sender<Result<T>>,
    this: &mut Actor,
    f: impl FnOnce(&mut Actor) -> Result<T>,
) -> Result<(), SendReplyError> {
    sender.send(f(this)).map_err(send_reply_error)
}

async fn send_reply_with_async<T>(
    sender: oneshot::Sender<Result<T>>,
    this: &mut Actor,
    f: impl AsyncFnOnce(&mut Actor) -> Result<T>,
) -> Result<(), SendReplyError> {
    sender.send(f(this).await).map_err(send_reply_error)
}

fn send_reply_error<T>(_err: T) -> SendReplyError {
    SendReplyError
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;
    #[tokio::test]
    async fn open_close() -> anyhow::Result<()> {
        let store = store::Store::memory();
        let sync = SyncHandle::spawn(store, None, None, None, "foo".into());
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();
        sync.import_namespace(namespace.into()).await?;
        sync.open(id, Default::default()).await?;
        let (tx, rx) = async_channel::bounded(10);
        sync.subscribe(id, tx).await?;
        sync.close(id).await?;
        assert!(rx.recv().await.is_err());
        Ok(())
    }

    /// Tests that streamer tasks spawned into `Actor.tasks` are reaped
    /// once they complete.
    ///
    /// The three streaming actions (`ListAuthors`, `ListReplicas`, and
    /// `ReplicaAction::GetMany`) each `spawn_local` a task into
    /// `Actor.tasks` to drive their reply channel. The actor must
    /// `join_next` those tasks once they finish, otherwise the
    /// `JoinSet` grows without bound for the lifetime of the actor.
    #[tokio::test]
    async fn actor_tasks_joinset_drain() -> anyhow::Result<()> {
        let store = store::Store::memory();
        let sync = SyncHandle::spawn(store, None, None, None, "drain".into());

        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();
        sync.import_namespace(namespace.into()).await?;
        sync.open(id, Default::default()).await?;

        const ITERATIONS: usize = 1000;

        for _ in 0..ITERATIONS {
            let (tx, mut rx) = mpsc::channel(64);
            sync.list_authors(tx).await?;
            while rx.recv().await?.is_some() {}
        }

        for _ in 0..ITERATIONS {
            let (tx, mut rx) = mpsc::channel(64);
            sync.list_replicas(tx).await?;
            while rx.recv().await?.is_some() {}
        }

        for _ in 0..ITERATIONS {
            let (tx, mut rx) = mpsc::channel(64);
            sync.get_many(id, store::Query::all().into(), tx).await?;
            while rx.recv().await?.is_some() {}
        }

        let mut last = sync.debug_tasks_len().await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while last > 16 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            last = sync.debug_tasks_len().await?;
        }

        assert!(
            last <= 16,
            "residual Actor.tasks JoinSet len = {last}, expected <= 16 \
             (was the join_next arm in run_async lost? streamer tasks \
             for ListAuthors / ListReplicas / GetMany are not being reaped)"
        );

        sync.close(id).await?;
        Ok(())
    }
}
