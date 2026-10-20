//! Explicit full-outage recovery authority for dynamically placed replicas.

use std::collections::HashMap;

use sha2::{Digest, Sha256};

use crate::shard::FragmentId;

/// Operator authorization to recover a complete dynamically placed replica set.
///
/// Configure the same fresh nonzero epoch on every surviving replica, retain
/// the same placement throughout recovery, and require stable storage. A node
/// remains non-serving until all candidates reconcile at an unchanged local
/// revision and the complete resulting version floor reaches stable storage.
/// This is not automatic election or permission to discard a missing replica.
#[derive(Clone, Debug, bon::Builder)]
#[non_exhaustive]
pub struct RecoveryOptions {
    epoch: [u8; 32],
}

impl RecoveryOptions {
    /// Construct explicit authority. Use a fresh operator-chosen epoch per outage.
    pub fn new(epoch: [u8; 32]) -> Result<Self, &'static str> {
        let options = Self { epoch };
        options.validate()?;
        Ok(options)
    }

    /// Non-secret recovery attempt identity. It is not a transport credential.
    pub fn epoch(&self) -> &[u8; 32] {
        &self.epoch
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.epoch == [0; 32] {
            Err("recovery epoch must be nonzero")
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RecoveryContext {
    options: RecoveryOptions,
    node_id: String,
    replicas: HashMap<FragmentId, Vec<String>>,
}

impl RecoveryContext {
    pub(crate) fn new(
        options: RecoveryOptions,
        node_id: String,
        replicas: HashMap<FragmentId, Vec<String>>,
    ) -> Self {
        Self {
            options,
            node_id,
            replicas,
        }
    }

    pub(crate) fn request_fields(&self, fragment: FragmentId) -> (Vec<u8>, Vec<u8>) {
        match self.replicas.get(&fragment) {
            Some(replicas) if replicas.contains(&self.node_id) => (
                self.options.epoch.to_vec(),
                replica_digest(fragment, replicas).to_vec(),
            ),
            _ => (Vec::new(), Vec::new()),
        }
    }

    pub(crate) fn authorizes(
        &self,
        fragment: FragmentId,
        peer: &str,
        epoch: &[u8],
        digest: &[u8],
        current: &HashMap<FragmentId, Vec<String>>,
    ) -> bool {
        if epoch != self.options.epoch || digest.len() != 32 {
            return false;
        }
        self.replicas
            .get(&fragment)
            .zip(current.get(&fragment))
            .is_some_and(|(expected, actual)| {
                expected.iter().any(|id| id == peer)
                    && expected.contains(&self.node_id)
                    && replica_digest(fragment, expected).as_slice() == digest
                    && replica_digest(fragment, actual).as_slice() == digest
            })
    }

    pub(crate) fn matches_placement(&self, fragment: FragmentId, current: &[String]) -> bool {
        self.replicas.get(&fragment).is_some_and(|expected| {
            replica_digest(fragment, expected) == replica_digest(fragment, current)
        })
    }
}

fn replica_digest(fragment: FragmentId, replicas: &[String]) -> [u8; 32] {
    let mut replicas: Vec<&str> = replicas.iter().map(String::as_str).collect();
    replicas.sort_unstable();
    let mut hash = Sha256::new();
    hash.update(b"actordb-recovery-replicas-v1\0");
    hash.update(fragment.0.to_be_bytes());
    hash.update((replicas.len() as u64).to_be_bytes());
    for replica in replicas {
        hash.update((replica.len() as u64).to_be_bytes());
        hash.update(replica.as_bytes());
    }
    hash.finalize().into()
}

/// Return the final mutation fence only after every durable candidate certifies
/// one unchanged local version floor. The caller retains it through publication.
pub(super) async fn bootstrap_all(
    deps: &super::PlacementDriverDeps,
    fragment: FragmentId,
    sources: &[String],
) -> Option<tokio::sync::OwnedRwLockWriteGuard<()>> {
    use crate::actor::{ApplyDurability, KvMessage};
    use crate::replication::SenderMsg;
    use ractor::rpc::CallResult;
    use std::sync::Arc;
    use std::time::Duration;

    tokio::time::timeout(super::BOOTSTRAP_TIMEOUT, async {
        loop {
            let (generation, actor) = deps.router.route_with_generation(fragment)?;
            let baseline = match actor
                .call(|reply| KvMessage::GetRevision { reply }, None)
                .await
            {
                Ok(CallResult::Success(revision)) => revision,
                _ => return None,
            };
            let candidates = {
                let senders = deps.senders.lock().await;
                let reports = deps.sender_reports.lock().await;
                sources
                    .iter()
                    .map(|source| {
                        let key = (fragment, source.clone());
                        Some((
                            key.clone(),
                            senders.get(&key)?.clone(),
                            reports.get(&key)?.clone(),
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?
            };
            let mut waiters = Vec::with_capacity(candidates.len());
            for (key, sender, report) in candidates {
                let rounds = report.lock().await.reconcile_rounds;
                sender.send(SenderMsg::ReconcileNow).ok()?;
                waiters.push((key, sender, report, rounds));
            }
            loop {
                let mut all_complete = true;
                for (key, sender, report, before) in &waiters {
                    let current = deps.senders.lock().await.get(key).cloned();
                    if sender.is_closed()
                        || current.is_none_or(|current| !current.same_task(sender))
                    {
                        return None;
                    }
                    all_complete &= report.lock().await.reconcile_rounds > *before;
                }
                if all_complete {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let fence = deps.router.mutation_gate(fragment).write_owned().await;
            if !deps.router.is_current(fragment, generation, &actor) {
                return None;
            }
            let current = match actor
                .call(|reply| KvMessage::GetRevision { reply }, None)
                .await
            {
                Ok(CallResult::Success(revision)) => revision,
                _ => return None,
            };
            if current != baseline {
                drop(fence);
                continue;
            }
            let stable = actor
                .call(|reply| KvMessage::FlushStable { reply }, None)
                .await;
            if !matches!(
                stable,
                Ok(CallResult::Success(Ok(ApplyDurability::StableCommitted)))
            ) {
                tracing::warn!(
                    fragment = fragment.0,
                    "recovery requires a successful stable local storage fence"
                );
                return None;
            }
            let current = match actor
                .call(|reply| KvMessage::GetRevision { reply }, None)
                .await
            {
                Ok(CallResult::Success(revision)) => revision,
                _ => return None,
            };
            if current != baseline {
                drop(fence);
                continue;
            }
            let registries_match = {
                let senders = deps.senders.lock().await;
                let reports = deps.sender_reports.lock().await;
                waiters.iter().all(|(key, sender, report, _)| {
                    senders
                        .get(key)
                        .is_some_and(|current| current.same_task(sender))
                        && reports
                            .get(key)
                            .is_some_and(|current| Arc::ptr_eq(current, report))
                })
            };
            if registries_match {
                return Some(fence);
            }
            drop(fence);
        }
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_is_bound_to_epoch_fragment_and_complete_replica_set() {
        let placement = HashMap::from([(FragmentId(0), vec!["a".into(), "b".into()])]);
        let context = RecoveryContext::new(
            RecoveryOptions::new([1; 32]).unwrap(),
            "a".into(),
            placement.clone(),
        );
        let (epoch, digest) = context.request_fields(FragmentId(0));
        assert!(context.authorizes(FragmentId(0), "b", &epoch, &digest, &placement));
        assert!(!context.authorizes(FragmentId(0), "outsider", &epoch, &digest, &placement));
        assert!(!context.authorizes(FragmentId(1), "b", &epoch, &digest, &placement));
        assert!(!context.authorizes(FragmentId(0), "b", &[2; 32], &digest, &placement));
        let changed = HashMap::from([(FragmentId(0), vec!["a".into(), "c".into()])]);
        assert!(!context.authorizes(FragmentId(0), "b", &epoch, &digest, &changed));
        assert!(RecoveryOptions::new([0; 32]).is_err());
    }
}
