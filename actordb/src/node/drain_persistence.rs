// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Owned, retryable local persistence work for application draining.

use super::drain::{DrainDurability, DrainError, FragmentDrainFence, LocalDurability};
use super::*;

type FenceResult = Result<FragmentDrainFence, String>;

struct FenceTicket {
    result: watch::Receiver<Option<FenceResult>>,
}

/// Tickets survive the caller's timeout. Their RPCs are registered with the
/// storage completion owner, and their actor retention survives reply loss.
pub(super) struct DrainPersistence {
    selected: StdMutex<Option<DrainDurability>>,
    tickets: StdMutex<HashMap<FragmentId, Arc<FenceTicket>>>,
    active: AtomicUsize,
    completed: AtomicUsize,
    changed: Notify,
    last_error: StdMutex<Option<(FragmentId, String)>>,
}

impl DrainPersistence {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            selected: StdMutex::new(None),
            tickets: StdMutex::new(HashMap::new()),
            active: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            changed: Notify::new(),
            last_error: StdMutex::new(None),
        })
    }

    pub(super) fn select(&self, requested: DrainDurability) -> Result<(), DrainError> {
        let mut selected = self.selected.lock().unwrap();
        match *selected {
            None => *selected = Some(requested),
            Some(current) if current == requested => {}
            Some(current) => {
                return Err(DrainError::IncompatibleRetry(format!(
                    "drain selected {current:?}; retry must preserve its durability (requested {requested:?})"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub(super) async fn wait_for_active(&self) {
        loop {
            let changed = self.changed.notified();
            if self.active() == 0 {
                return;
            }
            changed.await;
        }
    }

    pub(super) fn completed(&self) -> usize {
        self.completed.load(Ordering::Acquire)
    }

    pub(super) fn failure(&self) -> Option<(FragmentId, String)> {
        self.last_error
            .try_lock()
            .ok()
            .and_then(|error| error.clone())
    }

    pub(super) async fn complete(
        self: &Arc<Self>,
        node: &Node,
        requested: DrainDurability,
    ) -> Result<Vec<FragmentDrainFence>, DrainError> {
        if requested == DrainDurability::Applied {
            return Ok(Vec::new());
        }
        let stopped = node.operation_gate.phase.load(Ordering::SeqCst) == lifecycle::NODE_STOPPED;
        let targets = node
            .router
            .actors
            .read()
            .unwrap()
            .iter()
            .map(|(fragment, route)| (*fragment, route.generation, route.actor.clone()))
            .collect::<Vec<_>>();
        let mut fences = Vec::with_capacity(targets.len());
        for (fragment, generation, actor) in targets {
            let existing = self.tickets.lock().unwrap().get(&fragment).cloned();
            let ticket = match existing {
                Some(ticket) => ticket,
                None if stopped => {
                    // Ordinary shutdown preserves its existing configured-local
                    // contract, but cannot manufacture classified stable evidence.
                    if requested == DrainDurability::StableCommitted {
                        return Err(DrainError::Persistence {
                            fragment,
                            message: "node stopped without a retained stable drain receipt".into(),
                        });
                    }
                    continue;
                }
                None => {
                    let (reply, result) = watch::channel(None);
                    let ticket = Arc::new(FenceTicket { result });
                    self.tickets
                        .lock()
                        .unwrap()
                        .insert(fragment, ticket.clone());
                    self.active.fetch_add(1, Ordering::SeqCst);
                    let owner = self.clone();
                    let retention: Arc<dyn Send + Sync> = Arc::new(FenceOwnership(owner.clone()));
                    node.storage_executor.spawn_owner(async move {
                        let result =
                            execute_fence(fragment, generation, actor, requested, retention).await;
                        if result.is_ok() {
                            owner.completed.fetch_add(1, Ordering::Release);
                        }
                        reply.send_replace(Some(result));
                    });
                    ticket
                }
            };
            let mut result = ticket.result.clone();
            let fence = loop {
                if let Some(result) = result.borrow().clone() {
                    break result;
                }
                if result.changed().await.is_err() {
                    break Err("local persistence completion owner stopped".into());
                }
            };
            match fence {
                Ok(fence) => fences.push(fence),
                Err(message) => {
                    // The actor still owns a failed store batch. Reissue only its
                    // persistence fence on retry; do not replay application writes.
                    self.tickets.lock().unwrap().remove(&fragment);
                    *self.last_error.lock().unwrap() = Some((fragment, message.clone()));
                    return Err(DrainError::Persistence { fragment, message });
                }
            }
        }
        self.wait_for_active().await;
        *self.last_error.lock().unwrap() = None;
        fences.sort_by_key(|fence| fence.fragment);
        Ok(fences)
    }
}

struct FenceOwnership(Arc<DrainPersistence>);
impl Drop for FenceOwnership {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.changed.notify_waiters();
        }
    }
}

async fn execute_fence(
    fragment: FragmentId,
    generation: u64,
    actor: ActorRef<KvMessage>,
    requested: DrainDurability,
    retention: Arc<dyn Send + Sync>,
) -> FenceResult {
    let revision = match actor
        .call(
            |reply| {
                KvMessage::GetRevision { reply }.with_optional_retention(Some(retention.clone()))
            },
            None,
        )
        .await
    {
        Ok(CallResult::Success(revision)) => Some(revision),
        _ => None,
    };
    let result = actor
        .call(
            |reply| {
                let message = if requested == DrainDurability::StableCommitted {
                    KvMessage::FlushStable { reply }
                } else {
                    KvMessage::Flush { reply }
                };
                message.with_optional_retention(Some(retention))
            },
            None,
        )
        .await
        .map_err(|error| error.to_string())?;
    let actual = match result {
        CallResult::Success(Ok(ApplyDurability::MemoryApplied)) => LocalDurability::MemoryApplied,
        CallResult::Success(Ok(ApplyDurability::StoreCommitted)) => LocalDurability::StoreCommitted,
        CallResult::Success(Ok(ApplyDurability::StableCommitted)) => {
            LocalDurability::StableCommitted
        }
        CallResult::Success(Err(error)) => return Err(error.to_string()),
        CallResult::SenderError => return Err("persistence reply was lost".into()),
        CallResult::Timeout => return Err("owned persistence RPC unexpectedly timed out".into()),
    };
    if requested == DrainDurability::StableCommitted && actual != LocalDurability::StableCommitted {
        return Err(format!(
            "stable local storage required; actual fence was {actual:?}"
        ));
    }
    Ok(FragmentDrainFence {
        fragment,
        actor_generation: generation,
        revision,
        durability: actual,
    })
}
