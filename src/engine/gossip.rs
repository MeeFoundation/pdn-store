use std::collections::{hash_map, HashMap};

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::EndpointId;
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender, JoinOptions},
    net::Gossip,
};
use n0_future::{
    task::{AbortHandle, JoinSet},
    StreamExt,
};
use tokio::sync::mpsc;
use tracing::{debug, instrument, warn};

use super::live::{Op, ToLiveActor};
use crate::NamespaceId;

#[derive(Debug)]
struct ActiveState {
    sender: GossipSender,
    abort_handle: AbortHandle,
}

#[derive(Debug)]
pub struct GossipState {
    gossip: Gossip,
    to_live_actor: mpsc::Sender<ToLiveActor>,
    active: HashMap<NamespaceId, ActiveState>,
    active_tasks: JoinSet<(NamespaceId, Result<()>)>,
}

impl GossipState {
    pub fn new(gossip: Gossip, to_live_actor: mpsc::Sender<ToLiveActor>) -> Self {
        Self {
            gossip,
            to_live_actor,
            active: Default::default(),
            active_tasks: Default::default(),
        }
    }

    pub async fn join(&mut self, namespace: NamespaceId, bootstrap: Vec<EndpointId>) -> Result<()> {
        match self.active.entry(namespace) {
            hash_map::Entry::Occupied(mut entry) => {
                if !bootstrap.is_empty() {
                    entry.get_mut().sender.join_peers(bootstrap).await?;
                }
            }
            hash_map::Entry::Vacant(entry) => {
                let sub = self
                    .gossip
                    .subscribe_with_opts(namespace.into(), JoinOptions::with_bootstrap(bootstrap))
                    .await?;

                let (sender, stream) = sub.split();
                let to_live_actor = self.to_live_actor.clone();
                let abort_handle = self.active_tasks.spawn(async move {
                    let res = receive_loop(namespace, stream, to_live_actor).await;

                    (namespace, res)
                });
                entry.insert(ActiveState {
                    sender,
                    abort_handle,
                });
            }
        }
        Ok(())
    }

    pub fn quit(&mut self, topic: &NamespaceId) {
        if let Some(state) = self.active.remove(topic) {
            state.abort_handle.abort();
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        for (_, state) in self.active.drain() {
            state.abort_handle.abort();
        }
        self.progress().await
    }

    pub async fn broadcast_neighbors(&mut self, namespace: &NamespaceId, message: Bytes) {
        if let Some(state) = self.active.get_mut(namespace) {
            state.sender.broadcast_neighbors(message).await.ok();
        }
    }

    pub fn max_message_size(&self) -> usize {
        self.gossip.max_message_size()
    }

    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// Progress the internal task queues.
    ///
    /// Returns an error if any of the active tasks panic.
    ///
    /// ## Cancel safety
    ///
    /// This function is fully cancel-safe.
    pub async fn progress(&mut self) -> Result<()> {
        while let Some(res) = self.active_tasks.join_next().await {
            match res {
                Err(err) if err.is_cancelled() => continue,
                Err(err) => return Err(err).context("gossip receive loop panicked"),
                Ok((namespace, res)) => {
                    self.active.remove(&namespace);
                    if let Err(err) = res {
                        warn!(?err, ?namespace, "gossip receive loop failed")
                    }
                }
            }
        }
        Ok(())
    }
}

#[instrument("gossip-recv", skip_all, fields(namespace=%namespace.fmt_short()))]
async fn receive_loop(
    namespace: NamespaceId,
    mut recv: GossipReceiver,
    to_sync_actor: mpsc::Sender<ToLiveActor>,
) -> Result<()> {
    for peer in recv.neighbors() {
        to_sync_actor
            .send(ToLiveActor::NeighborUp { namespace, peer })
            .await?;
    }
    while let Some(event) = recv.try_next().await? {
        match event {
            Event::Lagged => {
                debug!("gossip loop lagged - dropping gossip event");
                continue;
            }
            Event::Received(msg) => {
                let op: Op = postcard::from_bytes(&msg.content)?;
                match op {
                    Op::Put(_entry) => {
                        // Content never rides the topic (content-free
                        // swarm): entries flow only over reconciliation,
                        // which the session access provider gates per peer.
                        // An entry broadcast — from a peer on an older
                        // build, or a malicious one trying to inject past
                        // the filter — is dropped, never inserted. Own
                        // writes announce their author head instead
                        // (`Op::SyncReport` from `broadcast_local_head`);
                        // the receiver pulls.
                        debug!(peer = %msg.delivered_from.fmt_short(), namespace = %namespace.fmt_short(), "dropping entry received via gossip: content does not ride the topic");
                    }
                    Op::ContentReady(hash) => {
                        to_sync_actor
                            .send(ToLiveActor::NeighborContentReady {
                                namespace,
                                node: msg.delivered_from,
                                hash,
                            })
                            .await?;
                    }
                    Op::SyncReport(report) => {
                        to_sync_actor
                            .send(ToLiveActor::IncomingSyncReport {
                                from: msg.delivered_from,
                                report,
                            })
                            .await?;
                    }
                }
            }
            Event::NeighborUp(peer) => {
                to_sync_actor
                    .send(ToLiveActor::NeighborUp { namespace, peer })
                    .await?;
            }
            Event::NeighborDown(peer) => {
                to_sync_actor
                    .send(ToLiveActor::NeighborDown { namespace, peer })
                    .await?;
            }
        }
    }
    Ok(())
}
