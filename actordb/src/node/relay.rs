// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Bounded forwarded-operation relays and cancellation ownership.
//!
//! Registry locks are synchronous and short: detach entries under the lock,
//! then notify or send after unlocking. A relay retains its count/key leases
//! through timer ownership and is pinned to exact origin/owner channels.
//! Connection replacement and caller cancellation reclaim the same lease once.

use super::*;

/// Relayed forwarded-gets keyed by their locally allocated request id.
pub(super) type RelayMap = Arc<StdMutex<HashMap<u64, GetRelay>>>;

/// Relayed forwarded mutations: relay req_id -> (origin peer_tx, origin req_id).
pub(super) type MutationRelayMap = Arc<StdMutex<HashMap<u64, MutationRelay>>>;

pub(super) struct MutationRelay {
    pub(super) expected_peer: String,
    pub(super) expected_tx: PeerChannel,
    pub(super) expected_keys: Vec<String>,
    pub(super) origin_tx: PeerChannel,
    pub(super) origin_req_id: u64,
    pub(super) timeout_cancel: oneshot::Sender<()>,
    /// Keep shutdown admission open until the downstream attempt reaches a
    /// response, timeout, or send failure. Response bytes may still be lost
    /// after enqueue; the fence covers mutation completion, not socket flush.
    pub(super) _operation: OperationPermit,
    pub(super) _admission: Arc<RelayLease>,
}

pub(super) struct MutationRelayRegistration {
    pub(super) expected_peer: String,
    pub(super) expected_tx: PeerChannel,
    pub(super) expected_keys: Vec<String>,
    pub(super) origin_tx: PeerChannel,
    pub(super) origin_req_id: u64,
    pub(super) deadline: Instant,
    pub(super) operation: OperationPermit,
}

pub(super) struct GetRelay {
    pub(super) expected_peer: String,
    pub(super) expected_tx: PeerChannel,
    pub(super) expected_key: String,
    pub(super) origin_tx: PeerChannel,
    pub(super) origin_req_id: u64,
    pub(super) timeout_cancel: oneshot::Sender<()>,
    pub(super) _admission: Arc<RelayLease>,
    pub(super) _operation: Option<OperationPermit>,
}

pub(super) struct GetRelayRegistration {
    pub(super) expected_peer: String,
    pub(super) expected_tx: PeerChannel,
    pub(super) expected_key: String,
    pub(super) origin_tx: PeerChannel,
    pub(super) origin_req_id: u64,
    pub(super) deadline: Instant,
    pub(super) operation: Option<OperationPermit>,
}

pub(super) struct RelayAdmission {
    pub(super) slots: Arc<Semaphore>,
    pub(super) key_bytes: Arc<Semaphore>,
}

pub(super) struct RelayLease {
    pub(super) _slot: OwnedSemaphorePermit,
    pub(super) _keys: OwnedSemaphorePermit,
}

impl RelayAdmission {
    pub(super) fn new(capacity: usize, key_byte_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(capacity)),
            key_bytes: Arc::new(Semaphore::new(key_byte_capacity)),
        })
    }

    pub(super) fn try_reserve(&self, key_bytes: usize) -> Option<Arc<RelayLease>> {
        let key_bytes = u32::try_from(key_bytes).ok()?;
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let keys = self
            .key_bytes
            .clone()
            .try_acquire_many_owned(key_bytes)
            .ok()?;
        Some(Arc::new(RelayLease {
            _slot: slot,
            _keys: keys,
        }))
    }
}

pub(super) fn relay_key_storage_bytes(capacity: usize) -> usize {
    std::mem::size_of::<String>().saturating_add(capacity)
}

pub(super) fn relay_keys_storage_bytes(keys: &[String], capacity: usize) -> usize {
    keys.iter().fold(
        std::mem::size_of::<Vec<String>>()
            .saturating_add(capacity.saturating_mul(std::mem::size_of::<String>())),
        |bytes, key| bytes.saturating_add(key.capacity()),
    )
}

pub(super) enum RelayRegistry {
    Get(RelayMap),
    Mutation(MutationRelayMap),
}

/// Synchronous rollback covers timer registration and outbound admission.
/// The timer also owns this lease, so its lifetime cannot outlive admission.
pub(super) struct RelayRegistrationGuard {
    pub(super) registry: RelayRegistry,
    pub(super) relay_id: u64,
    pub(super) admission: Arc<RelayLease>,
    pub(super) origin_tx: PeerChannel,
    pub(super) armed: bool,
}

impl RelayRegistrationGuard {
    pub(super) fn disarm(mut self) {
        self.armed = false;
    }

    pub(super) async fn enqueue(
        &self,
        owner_tx: &PeerChannel,
        envelope: Envelope,
        deadline: Instant,
    ) -> bool {
        let mut owner_cancel = owner_tx.cancel.subscribe();
        let mut origin_cancel = self.origin_tx.cancel.subscribe();
        if *owner_cancel.borrow() || *origin_cancel.borrow() {
            return false;
        }
        tokio::select! {
            _ = owner_cancel.changed() => false,
            _ = origin_cancel.changed() => false,
            sent = try_send_envelope_until(owner_tx, envelope, deadline) => sent,
        }
    }
}

impl Drop for RelayRegistrationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        match &self.registry {
            RelayRegistry::Get(relays) => {
                drop(remove_get_relay(relays, self.relay_id, &self.admission));
            }
            RelayRegistry::Mutation(relays) => {
                if let Some(relay) = remove_mutation_relay(relays, self.relay_id, &self.admission) {
                    fail_mutation_relay(relay, "mutation relay canceled before enqueue");
                }
            }
        }
    }
}

pub(super) fn remove_get_relay(
    relays: &RelayMap,
    id: u64,
    admission: &Arc<RelayLease>,
) -> Option<GetRelay> {
    let mut relays = relays.lock().unwrap();
    if relays
        .get(&id)
        .is_some_and(|relay| Arc::ptr_eq(&relay._admission, admission))
    {
        relays.remove(&id)
    } else {
        None
    }
}

pub(super) fn remove_mutation_relay(
    relays: &MutationRelayMap,
    id: u64,
    admission: &Arc<RelayLease>,
) -> Option<MutationRelay> {
    let mut relays = relays.lock().unwrap();
    if relays
        .get(&id)
        .is_some_and(|relay| Arc::ptr_eq(&relay._admission, admission))
    {
        relays.remove(&id)
    } else {
        None
    }
}

pub(super) fn fail_mutation_relay(relay: MutationRelay, reason: &str) {
    let MutationRelay {
        origin_tx,
        origin_req_id,
        expected_keys,
        timeout_cancel,
        _operation,
        _admission,
        ..
    } = relay;
    let _ = timeout_cancel.send(());
    if !*origin_tx.cancel.borrow() {
        let _ = send_unavailable_mutation(&origin_tx, origin_req_id, expected_keys, reason);
    }
}

pub(super) fn cancel_connection_relays(
    channel: &PeerChannel,
    reads: &RelayMap,
    mutations: &MutationRelayMap,
) {
    let reads = {
        let mut reads = reads.lock().unwrap();
        let ids: Vec<_> = reads
            .iter()
            .filter_map(|(id, relay)| {
                (relay.origin_tx.same_channel(channel) || relay.expected_tx.same_channel(channel))
                    .then_some(*id)
            })
            .collect();
        ids.into_iter()
            .filter_map(|id| reads.remove(&id))
            .collect::<Vec<_>>()
    };
    drop(reads);
    let mutations = {
        let mut mutations = mutations.lock().unwrap();
        let ids: Vec<_> = mutations
            .iter()
            .filter_map(|(id, relay)| {
                (relay.origin_tx.same_channel(channel) || relay.expected_tx.same_channel(channel))
                    .then_some(*id)
            })
            .collect();
        ids.into_iter()
            .filter_map(|id| mutations.remove(&id))
            .collect::<Vec<_>>()
    };
    for relay in mutations {
        fail_mutation_relay(relay, "mutation relay connection closed");
    }
}

/// Shared context for re-forwarding ops during migration (Phase 8).
/// A node that receives a routed op for a fragment it no longer
/// serves re-forwards it to the current owner instead of dropping it.
pub(super) struct ReforwardCtx {
    pub(super) node_id: String,
    pub(super) placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    pub(super) peers: PeerMap,
    pub(super) tasks: Weak<RuntimeTasks>,
    pub(super) pending_relays: RelayMap,
    pub(super) pending_mutation_relays: MutationRelayMap,
    pub(super) next_req_id: Arc<AtomicU64>,
    pub(super) forwarded_attempt_timeout: Duration,
    pub(super) relay_admission: Arc<RelayAdmission>,
}

/// The current owner for a re-forward: not us, and not the peer the op
/// arrived from (loop protection; `hops` is the backstop).
pub(super) async fn reforward_target(
    ctx: &ReforwardCtx,
    peer_node_id: &Option<String>,
    frag: FragmentId,
) -> Option<(String, PeerChannel)> {
    let replicas = ctx.placement.read().unwrap().get(&frag).cloned()?;
    let peers = ctx.peers.lock().await;
    for owner in replicas {
        if owner == ctx.node_id || peer_node_id.as_deref() == Some(owner.as_str()) {
            continue;
        }
        if let Some(tx) = peers.get(&owner) {
            return Some((owner, tx.clone()));
        }
    }
    None
}

pub(super) fn unavailable_mutation_results(keys: Vec<String>, reason: &str) -> Vec<MutationResult> {
    keys.into_iter()
        .enumerate()
        .map(|(request_index, key)| {
            mutation_result(
                key,
                request_index,
                Err(NodeError::Network(reason.to_string())),
            )
        })
        .collect()
}

pub(super) fn send_unavailable_mutation(
    tx: &PeerChannel,
    req_id: u64,
    keys: Vec<String>,
    reason: &str,
) -> bool {
    send_mutation_response(
        tx,
        MutationResponse {
            req_id,
            results: unavailable_mutation_results(keys, reason),
        },
    )
}

pub(super) fn send_mutation_response(tx: &PeerChannel, response: MutationResponse) -> bool {
    try_send_envelope_immediate(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::MutationResponse(response)),
        },
    )
}

pub(super) fn send_batch_get_response(
    tx: &PeerChannel,
    req_id: u64,
    results: Vec<BatchKeyValue>,
) -> bool {
    try_send_envelope_immediate(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::BatchGetResponse(BatchGetResponse {
                req_id,
                results,
            })),
        },
    )
}

pub(super) async fn register_mutation_relay(
    tasks: &Weak<RuntimeTasks>,
    relays: MutationRelayMap,
    admission: &Arc<RelayAdmission>,
    relay_id: u64,
    registration: MutationRelayRegistration,
) -> Option<RelayRegistrationGuard> {
    let lease = admission.try_reserve(relay_keys_storage_bytes(
        &registration.expected_keys,
        registration.expected_keys.capacity(),
    ));
    let Some(lease) = lease else {
        let MutationRelayRegistration {
            origin_tx,
            origin_req_id,
            expected_keys,
            operation: _operation,
            ..
        } = registration;
        let _ = send_unavailable_mutation(
            &origin_tx,
            origin_req_id,
            expected_keys,
            "mutation relay admission exhausted",
        );
        return None;
    };
    let MutationRelayRegistration {
        expected_peer,
        expected_tx,
        expected_keys,
        origin_tx,
        origin_req_id,
        deadline,
        operation,
    } = registration;
    let (timeout_cancel, cancelled) = oneshot::channel();
    let mut owner_cancel = expected_tx.cancel.subscribe();
    let mut origin_cancel = origin_tx.cancel.subscribe();
    let guard = RelayRegistrationGuard {
        registry: RelayRegistry::Mutation(relays.clone()),
        relay_id,
        admission: lease.clone(),
        origin_tx: origin_tx.clone(),
        armed: true,
    };
    let relay = MutationRelay {
        expected_peer,
        expected_tx,
        expected_keys,
        origin_tx,
        origin_req_id,
        timeout_cancel,
        _operation: operation,
        _admission: lease.clone(),
    };
    if *owner_cancel.borrow() || *origin_cancel.borrow() || Instant::now() >= deadline {
        fail_mutation_relay(
            relay,
            "mutation relay connection closed before registration",
        );
        return None;
    }
    {
        let mut entries = relays.lock().unwrap();
        if entries.contains_key(&relay_id) {
            drop(entries);
            fail_mutation_relay(relay, "mutation relay request id collision");
            return None;
        }
        entries.insert(relay_id, relay);
    }

    // Register before spawning the timer. The cancellation sender is already
    // in the map, so every registered relay has exactly one live deadline.
    let timeout_relays = relays.clone();
    let timeout_task = async move {
        // Admission is retained until the timer future itself exits, not just
        // until its map entry is removed. Completed/canceled timers are bounded.
        let _timer_admission = lease;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                if let Some(relay) = remove_mutation_relay(&timeout_relays, relay_id, &_timer_admission) {
                    fail_mutation_relay(relay, "forwarded mutation relay timed out");
                }
            }
            _ = owner_cancel.changed() => {
                if let Some(relay) = remove_mutation_relay(&timeout_relays, relay_id, &_timer_admission) {
                    fail_mutation_relay(relay, "mutation relay owner connection closed");
                }
            }
            _ = origin_cancel.changed() => {
                if let Some(relay) = remove_mutation_relay(&timeout_relays, relay_id, &_timer_admission) {
                    fail_mutation_relay(relay, "mutation relay origin connection closed");
                }
            }
            _ = cancelled => {}
        }
    };
    let registered = match tasks.upgrade() {
        Some(tasks) => tokio::time::timeout_at(deadline, tasks.spawn(timeout_task))
            .await
            .unwrap_or(false),
        None => false,
    };
    registered.then_some(guard)
}

pub(super) async fn register_get_relay(
    tasks: &Weak<RuntimeTasks>,
    relays: RelayMap,
    admission: &Arc<RelayAdmission>,
    relay_id: u64,
    registration: GetRelayRegistration,
) -> Option<RelayRegistrationGuard> {
    let lease = admission.try_reserve(relay_key_storage_bytes(
        registration.expected_key.capacity(),
    ))?;
    let GetRelayRegistration {
        expected_peer,
        expected_tx,
        expected_key,
        origin_tx,
        origin_req_id,
        deadline,
        operation,
    } = registration;
    let (timeout_cancel, cancelled) = oneshot::channel();
    let mut owner_cancel = expected_tx.cancel.subscribe();
    let mut origin_cancel = origin_tx.cancel.subscribe();
    if *owner_cancel.borrow() || *origin_cancel.borrow() || Instant::now() >= deadline {
        return None;
    }
    let guard = RelayRegistrationGuard {
        registry: RelayRegistry::Get(relays.clone()),
        relay_id,
        admission: lease.clone(),
        origin_tx: origin_tx.clone(),
        armed: true,
    };
    {
        let mut entries = relays.lock().unwrap();
        if entries.contains_key(&relay_id) {
            return None;
        }
        entries.insert(
            relay_id,
            GetRelay {
                expected_peer,
                expected_tx,
                expected_key,
                origin_tx,
                origin_req_id,
                timeout_cancel,
                _admission: lease.clone(),
                _operation: operation,
            },
        );
    }
    let timeout_relays = relays.clone();
    let timeout_task = async move {
        let _timer_admission = lease;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                drop(remove_get_relay(&timeout_relays, relay_id, &_timer_admission));
            }
            _ = owner_cancel.changed() => {
                drop(remove_get_relay(&timeout_relays, relay_id, &_timer_admission));
            }
            _ = origin_cancel.changed() => {
                drop(remove_get_relay(&timeout_relays, relay_id, &_timer_admission));
            }
            _ = cancelled => {}
        }
    };
    let registered = match tasks.upgrade() {
        Some(tasks) => tokio::time::timeout_at(deadline, tasks.spawn(timeout_task))
            .await
            .unwrap_or(false),
        None => false,
    };
    registered.then_some(guard)
}

/// Relay a write to the fragment's current owner and preserve the exact
/// response path. A fresh local request id prevents collisions between
/// independent upstream peers.
pub(super) async fn relay_put(
    mut p: ForwardedPut,
    frag: FragmentId,
    origin_tx: &PeerChannel,
    peer_node_id: &Option<String>,
    ctx: &ReforwardCtx,
    operation: OperationPermit,
) {
    let original_req_id = p.req_id;
    let Some((owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        let _ = send_unavailable_mutation(
            origin_tx,
            original_req_id,
            vec![p.key],
            "no connected owner available for mutation relay",
        );
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    let extra = match p
        .write_concern
        .as_deref()
        .map(crate::write_concern::WriteConcernOptions::decode)
        .transpose()
    {
        Ok(options) => options.map_or(Duration::ZERO, |options| options.timeout()),
        Err(error) => {
            let failure = crate::write_concern::WriteConcernError::from(error);
            let mut result = mutation_result(p.key, 0, Err(NodeError::Codec(failure.to_string())));
            result.write_concern = failure.outcome().map(|outcome| outcome.encode());
            let _ = send_mutation_response(
                origin_tx,
                MutationResponse {
                    req_id: original_req_id,
                    results: vec![result],
                },
            );
            return;
        }
    };
    let Some(timeout) = ctx.forwarded_attempt_timeout.checked_add(extra) else {
        let _ = send_unavailable_mutation(
            origin_tx,
            original_req_id,
            vec![p.key],
            "relay concern timeout exceeds range",
        );
        return;
    };
    let Some(attempt_deadline) = Instant::now().checked_add(timeout) else {
        let _ = send_unavailable_mutation(
            origin_tx,
            original_req_id,
            vec![p.key],
            "relay concern timeout exceeds range",
        );
        return;
    };
    let keys = vec![p.key.clone()];
    let Some(registration) = register_mutation_relay(
        &ctx.tasks,
        ctx.pending_mutation_relays.clone(),
        &ctx.relay_admission,
        relay_id,
        MutationRelayRegistration {
            expected_peer: owner,
            expected_tx: owner_tx.clone(),
            expected_keys: keys,
            origin_tx: origin_tx.clone(),
            origin_req_id: original_req_id,
            deadline: attempt_deadline,
            operation,
        },
    )
    .await
    else {
        return;
    };
    p.req_id = relay_id;
    p.hops += 1;
    let sent = registration
        .enqueue(
            &owner_tx,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(p)),
            },
            attempt_deadline,
        )
        .await;
    if !sent {
        drop(registration);
    } else {
        registration.disarm();
    }
}

/// Relay a whole single-fragment batch with the same response guarantees.
pub(super) async fn relay_batch(
    mut bp: BatchPut,
    frag: FragmentId,
    origin_tx: &PeerChannel,
    peer_node_id: &Option<String>,
    ctx: &ReforwardCtx,
    operation: OperationPermit,
) {
    let original_req_id = bp.req_id;
    let Some((owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        let _ = send_unavailable_mutation(
            origin_tx,
            original_req_id,
            bp.puts.into_iter().map(|put| put.key).collect(),
            "no connected owner available for batch relay",
        );
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    let attempt_deadline = deadline_after(ctx.forwarded_attempt_timeout);
    let keys: Vec<String> = bp.puts.iter().map(|put| put.key.clone()).collect();
    let Some(registration) = register_mutation_relay(
        &ctx.tasks,
        ctx.pending_mutation_relays.clone(),
        &ctx.relay_admission,
        relay_id,
        MutationRelayRegistration {
            expected_peer: owner,
            expected_tx: owner_tx.clone(),
            expected_keys: keys,
            origin_tx: origin_tx.clone(),
            origin_req_id: original_req_id,
            deadline: attempt_deadline,
            operation,
        },
    )
    .await
    else {
        return;
    };
    bp.req_id = relay_id;
    for p in bp.puts.iter_mut() {
        p.hops += 1;
    }
    let sent = registration
        .enqueue(
            &owner_tx,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(bp)),
            },
            attempt_deadline,
        )
        .await;
    if !sent {
        drop(registration);
    } else {
        registration.disarm();
    }
}

/// Relay a read for a fragment we don't serve to the current owner,
/// remembering where to send the response. Hop-limited and never
/// bounced back to the peer it arrived from.
pub(super) async fn relay_get(
    g: &ForwardedGet,
    frag: FragmentId,
    origin_tx: &PeerChannel,
    peer_node_id: &Option<String>,
    ctx: &ReforwardCtx,
    operation: OperationPermit,
) {
    let Some((owner, owner_tx)) = reforward_target(ctx, peer_node_id, frag).await else {
        return;
    };
    let relay_id = ctx.next_req_id.fetch_add(1, Ordering::Relaxed);
    let attempt_deadline = deadline_after(ctx.forwarded_attempt_timeout);
    let Some(registration) = register_get_relay(
        &ctx.tasks,
        ctx.pending_relays.clone(),
        &ctx.relay_admission,
        relay_id,
        GetRelayRegistration {
            expected_peer: owner,
            expected_tx: owner_tx.clone(),
            expected_key: g.key.clone(),
            origin_tx: origin_tx.clone(),
            origin_req_id: g.req_id,
            deadline: attempt_deadline,
            operation: Some(operation),
        },
    )
    .await
    else {
        return;
    };
    let sent = registration
        .enqueue(
            &owner_tx,
            Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key: g.key.clone(),
                    req_id: relay_id,
                    // Preserve the originator's priority across the relay.
                    priority: g.priority,
                    hops: g.hops + 1,
                })),
            },
            attempt_deadline,
        )
        .await;
    if !sent {
        drop(registration);
    } else {
        registration.disarm();
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn mutation_relay_timeout_removes_entry_and_replies_unavailable() {
        let relays: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (origin_tx, _origin_rx, mut origin_response_rx) = test_peer_channel(1);
        let (expected_tx, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let admission = RelayAdmission::new(1, 1024);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let operation_gate = OperationGate::new();
        let operation = operation_gate.try_enter().unwrap();
        register_mutation_relay(
            &task_ref,
            relays.clone(),
            &admission,
            17,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx,
                expected_keys: vec!["key".to_string()],
                origin_tx,
                origin_req_id: 9,
                deadline: Instant::now() + Duration::from_millis(50),
                operation,
            },
        )
        .await
        .unwrap()
        .disarm();
        assert!(relays.lock().unwrap().contains_key(&17));
        operation_gate.begin_shutdown();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), operation_gate.wait_for_drain())
                .await
                .is_err(),
            "relay released mutation admission before response or timeout"
        );

        tokio::time::advance(Duration::from_millis(50)).await;
        let bytes = tokio::time::timeout(Duration::from_secs(1), origin_response_rx.recv())
            .await
            .expect("relay timeout did not reply")
            .expect("relay response channel closed");
        let envelope = Envelope::decode(bytes.bytes()).expect("invalid relay response");
        let Some(EnvelopeMsg::MutationResponse(response)) = envelope.msg else {
            panic!("expected mutation response");
        };
        assert_eq!(response.req_id, 9);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].key, "key");
        assert_eq!(
            MutationStatus::try_from(response.results[0].status),
            Ok(MutationStatus::Unavailable)
        );
        assert!(relays.lock().unwrap().is_empty());
        tokio::time::timeout(Duration::from_secs(1), operation_gate.wait_for_drain())
            .await
            .expect("relay timeout did not release mutation admission");
        tasks.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn completed_mutation_relay_cancels_timeout() {
        let relays: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (origin_tx, _origin_rx, mut origin_response_rx) = test_peer_channel(1);
        let (expected_tx, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let admission = RelayAdmission::new(1, 1024);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let operation_gate = OperationGate::new();
        let operation = operation_gate.try_enter().unwrap();
        register_mutation_relay(
            &task_ref,
            relays.clone(),
            &admission,
            17,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx,
                expected_keys: vec!["key".to_string()],
                origin_tx,
                origin_req_id: 9,
                deadline: Instant::now() + Duration::from_millis(10),
                operation,
            },
        )
        .await
        .unwrap()
        .disarm();
        let relay = relays.lock().unwrap().remove(&17).expect("relay missing");
        let MutationRelay {
            timeout_cancel,
            _operation: operation,
            _admission: relay_admission,
            ..
        } = relay;
        let _ = timeout_cancel.send(());
        operation_gate.begin_shutdown();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), operation_gate.wait_for_drain())
                .await
                .is_err(),
            "removed relay released mutation admission before response handling finished"
        );
        drop(operation);
        drop(relay_admission);
        tokio::time::timeout(Duration::from_secs(1), operation_gate.wait_for_drain())
            .await
            .expect("completed relay did not release mutation admission");

        tokio::time::advance(Duration::from_millis(30)).await;
        // A removed entry alone does not prove that its canceled timer exited.
        // The timer retains the sole admission slot until its future drops.
        let _timer_exited = tokio::time::timeout(Duration::from_secs(1), admission.slots.acquire())
            .await
            .expect("completed relay timer did not release admission")
            .expect("relay admission closed");
        assert!(origin_response_rx.try_recv().is_err());
        assert!(relays.lock().unwrap().is_empty());
        tasks.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn read_relay_timeout_removes_registry_entry() {
        let relays: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (origin_tx, _origin_rx, _origin_response_rx) = test_peer_channel(1);
        let (expected_tx, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let admission = RelayAdmission::new(1, 1024);
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        register_get_relay(
            &task_ref,
            relays.clone(),
            &admission,
            23,
            GetRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx,
                expected_key: "key".to_string(),
                origin_tx,
                origin_req_id: 11,
                operation: None,
                deadline: Instant::now() + Duration::from_millis(10),
            },
        )
        .await
        .unwrap()
        .disarm();
        assert!(relays.lock().unwrap().contains_key(&23));
        tokio::time::advance(Duration::from_millis(10)).await;
        let _timer_exited = tokio::time::timeout(Duration::from_secs(1), admission.slots.acquire())
            .await
            .expect("relay deadline timer did not release admission")
            .expect("relay admission closed");
        assert!(relays.lock().unwrap().is_empty());
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn origin_cancellation_reclaims_relays_without_connection_cleanup() {
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let admission = RelayAdmission::new(2, 1024);
        let reads: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (owner, _owner_rx, _owner_responses) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_responses) = test_peer_channel(1);
        let gate = OperationGate::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            1,
            GetRelayRegistration {
                expected_peer: "owner".into(),
                expected_tx: owner.clone(),
                expected_key: "key".into(),
                origin_tx: origin.clone(),
                origin_req_id: 11,
                operation: None,
                deadline,
            },
        )
        .await
        .unwrap()
        .disarm();
        register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            2,
            MutationRelayRegistration {
                expected_peer: "owner".into(),
                expected_tx: owner.clone(),
                expected_keys: vec!["key".into(), "key".into()],
                origin_tx: origin.clone(),
                origin_req_id: 12,
                deadline,
                operation: gate.try_enter().unwrap(),
            },
        )
        .await
        .unwrap()
        .disarm();
        assert_eq!(admission.slots.available_permits(), 0);
        gate.begin_shutdown();
        let mut drain = Box::pin(gate.wait_for_drain());
        assert!(futures::poll!(drain.as_mut()).is_pending());

        // The reader can be aborted before reaching its cleanup tail. Only
        // the registered timers observe this origin's terminal cancellation.
        origin.cancel();
        wait_for_relay_admission(&admission, 2, 1024).await;
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .expect("origin cancellation retained the mutation operation fence");
        assert!(reads.lock().unwrap().is_empty());
        assert!(mutations.lock().unwrap().is_empty());
        assert!(origin_responses.try_recv().is_err());
        assert!(
            !*owner.cancel.borrow(),
            "origin loss canceled a healthy owner"
        );
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn relay_cleanup_is_pinned_to_exact_owner_origin_and_registration_generation() {
        let tasks = RuntimeTasks::new(8);
        let task_ref = Arc::downgrade(&tasks);
        let admission = RelayAdmission::new(8, 4096);
        let reads: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (old_owner, _old_owner_rx, _old_owner_responses) = test_peer_channel(1);
        let (new_owner, _new_owner_rx, _new_owner_responses) = test_peer_channel(1);
        let (old_origin, _old_origin_rx, mut old_origin_responses) = test_peer_channel(4);
        let (new_origin, _new_origin_rx, mut new_origin_responses) = test_peer_channel(4);
        let gate = OperationGate::new();
        for (id, owner, origin) in [
            (1, &old_owner, &new_origin),
            (2, &new_owner, &new_origin),
            (3, &new_owner, &old_origin),
        ] {
            register_get_relay(
                &task_ref,
                reads.clone(),
                &admission,
                id,
                GetRelayRegistration {
                    expected_peer: "same-owner-id".to_string(),
                    expected_tx: owner.clone(),
                    expected_key: "key".to_string(),
                    origin_tx: origin.clone(),
                    origin_req_id: id + 100,
                    operation: None,
                    deadline: Instant::now() + Duration::from_secs(10),
                },
            )
            .await
            .unwrap()
            .disarm();
            register_mutation_relay(
                &task_ref,
                mutations.clone(),
                &admission,
                id,
                MutationRelayRegistration {
                    expected_peer: "same-owner-id".to_string(),
                    expected_tx: owner.clone(),
                    expected_keys: vec!["key".to_string()],
                    origin_tx: origin.clone(),
                    origin_req_id: id + 200,
                    deadline: Instant::now() + Duration::from_secs(10),
                    operation: gate.try_enter().unwrap(),
                },
            )
            .await
            .unwrap()
            .disarm();
        }
        // Old physical owner cleanup must not clear replacement work which
        // carries the exact same authenticated peer id.
        old_owner.cancel();
        cancel_connection_relays(&old_owner, &reads, &mutations);
        assert!(!reads.lock().unwrap().contains_key(&1));
        assert!(!mutations.lock().unwrap().contains_key(&1));
        assert_eq!(reads.lock().unwrap().len(), 2);
        assert_eq!(mutations.lock().unwrap().len(), 2);
        let unavailable = new_origin_responses.try_recv().unwrap();
        assert!(matches!(Envelope::decode(unavailable.bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 201 && response.results[0].status == MutationStatus::Unavailable as i32));
        old_origin.cancel();
        cancel_connection_relays(&old_origin, &reads, &mutations);
        assert_eq!(reads.lock().unwrap().len(), 1);
        assert_eq!(mutations.lock().unwrap().len(), 1);
        assert!(reads.lock().unwrap().contains_key(&2));
        assert!(mutations.lock().unwrap().contains_key(&2));
        assert!(old_origin_responses.try_recv().is_err());
        cancel_connection_relays(&old_owner, &reads, &mutations);
        assert!(reads.lock().unwrap().contains_key(&2));
        assert!(mutations.lock().unwrap().contains_key(&2));

        // A dropped registration guard/timer may race request-id reuse. It
        // must compare its lease generation rather than delete a new entry.
        let stale_guard = register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            4,
            GetRelayRegistration {
                expected_peer: "same-owner-id".to_string(),
                expected_tx: new_owner.clone(),
                expected_key: "old".to_string(),
                origin_tx: new_origin.clone(),
                origin_req_id: 104,
                operation: None,
                deadline: Instant::now() + Duration::from_secs(10),
            },
        )
        .await
        .unwrap();
        let removed = reads.lock().unwrap().remove(&4).unwrap();
        drop(removed);
        register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            4,
            GetRelayRegistration {
                expected_peer: "same-owner-id".to_string(),
                expected_tx: new_owner.clone(),
                expected_key: "new".to_string(),
                origin_tx: new_origin,
                origin_req_id: 105,
                operation: None,
                deadline: Instant::now() + Duration::from_secs(10),
            },
        )
        .await
        .unwrap()
        .disarm();
        drop(stale_guard);
        assert_eq!(reads.lock().unwrap().get(&4).unwrap().origin_req_id, 105);
        new_owner.cancel();
        // Exercise timer-owned disconnect cleanup, even if the reader cannot
        // reach its own tail. No explicit map cleanup for this final channel.
        wait_for_relay_admission(&admission, 8, 4096).await;
        assert!(reads.lock().unwrap().is_empty());
        assert!(mutations.lock().unwrap().is_empty());
        gate.begin_shutdown();
        gate.wait_for_drain().await;
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn relay_responses_ignore_same_peer_on_a_different_physical_channel() {
        let (ctx, tasks) = test_connection_context(
            FragmentRouter::new(1, 0),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashMap::new())),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        )
        .await;
        let (mut owner, server) = tokio::io::duplex(4096);
        let connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut owner, "owner").await;
        let physical_owner = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(tx) = ctx.peers.lock().await.get("owner").cloned() {
                    break tx;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (different_physical_owner, _other_rx, _other_responses) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_responses) = test_peer_channel(4);
        register_get_relay(
            &ctx.tasks,
            ctx.pending_relays.clone(),
            &ctx.relay_admission,
            11,
            GetRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: different_physical_owner.clone(),
                expected_key: "key".to_string(),
                origin_tx: origin.clone(),
                origin_req_id: 101,
                operation: None,
                deadline: Instant::now() + Duration::from_secs(10),
            },
        )
        .await
        .unwrap()
        .disarm();
        register_mutation_relay(
            &ctx.tasks,
            ctx.pending_mutation_relays.clone(),
            &ctx.relay_admission,
            12,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: different_physical_owner.clone(),
                expected_keys: vec!["key".to_string()],
                origin_tx: origin,
                origin_req_id: 102,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: ctx.operation_gate.try_enter().unwrap(),
            },
        )
        .await
        .unwrap()
        .disarm();
        let read_response = Envelope {
            msg: Some(EnvelopeMsg::GetResponse(GetResponse {
                req_id: 11,
                found: false,
                value: vec![],
                timestamp_ms: 0,
                logical_counter: 0,
                node_id: String::new(),
                expires_at_ms: 0,
                is_tombstone: false,
                shed: false,
            })),
        };
        let mutation_response = Envelope {
            msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                req_id: 12,
                results: vec![result("key", 0)],
            })),
        };
        for barrier in [201, 202] {
            write_test_envelope(&mut owner, read_response.clone()).await;
            write_test_envelope(&mut owner, mutation_response.clone()).await;
            write_test_envelope(
                &mut owner,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: barrier,
                        keys: vec![],
                        priority: 0,
                    })),
                },
            )
            .await;
            assert!(matches!(read_test_envelope(&mut owner).await.msg,
                Some(EnvelopeMsg::BatchGetResponse(response)) if response.req_id == barrier));
            if barrier == 201 {
                assert!(ctx.pending_relays.lock().unwrap().contains_key(&11));
                assert!(ctx
                    .pending_mutation_relays
                    .lock()
                    .unwrap()
                    .contains_key(&12));
                assert!(origin_responses.try_recv().is_err());
                ctx.pending_relays
                    .lock()
                    .unwrap()
                    .get_mut(&11)
                    .unwrap()
                    .expected_tx = physical_owner.clone();
                ctx.pending_mutation_relays
                    .lock()
                    .unwrap()
                    .get_mut(&12)
                    .unwrap()
                    .expected_tx = physical_owner.clone();
            }
        }
        assert!(
            matches!(Envelope::decode(origin_responses.try_recv().unwrap().bytes()).unwrap().msg,
            Some(EnvelopeMsg::GetResponse(response)) if response.req_id == 101)
        );
        assert!(
            matches!(Envelope::decode(origin_responses.try_recv().unwrap().bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 102)
        );
        wait_for_relay_admission(
            &ctx.relay_admission,
            RELAY_CAPACITY,
            RELAY_KEY_BYTE_CAPACITY,
        )
        .await;
        drop(owner);
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .unwrap()
            .unwrap();
        tasks.shutdown().await;
    }

    #[test]
    fn relay_key_admission_charges_empty_keys_and_unused_storage_and_rolls_back() {
        let mut keys = Vec::with_capacity(8);
        keys.push(String::with_capacity(64));
        keys.push(String::new());
        let bytes = relay_keys_storage_bytes(&keys, keys.capacity());
        assert_eq!(
            bytes,
            std::mem::size_of::<Vec<String>>()
                + keys.capacity() * std::mem::size_of::<String>()
                + 64
        );
        assert_eq!(
            relay_key_storage_bytes(64),
            std::mem::size_of::<String>() + 64
        );
        let admission = RelayAdmission::new(2, bytes);
        assert!(admission.try_reserve(bytes + 1).is_none());
        assert_eq!(
            admission.slots.available_permits(),
            2,
            "failed byte admission leaked a count slot"
        );
        let lease = admission.try_reserve(bytes).unwrap();
        assert!(admission.try_reserve(1).is_none());
        assert_eq!(admission.slots.available_permits(), 1);
        let timer_lease = lease.clone();
        drop(lease);
        assert_eq!(
            admission.key_bytes.available_permits(),
            0,
            "timer lifetime must retain admission"
        );
        drop(timer_lease);
        assert_eq!(admission.slots.available_permits(), 2);
        assert_eq!(admission.key_bytes.available_permits(), bytes);
    }

    #[tokio::test]
    async fn pre_canceled_owner_rejects_relay_registration_without_waiting() {
        let admission = RelayAdmission::new(2, 1024);
        let reads: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let (owner, _owner_rx, _owner_responses) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_responses) = test_peer_channel(1);
        let gate = OperationGate::new();
        // No cancellation receivers exist yet: terminal cancellation must be
        // sticky for a relay which starts after the old reader has exited.
        owner.cancel();
        assert!(*owner.cancel.subscribe().borrow());
        assert!(register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            1,
            GetRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner.clone(),
                expected_key: "key".to_string(),
                origin_tx: origin.clone(),
                origin_req_id: 11,
                operation: None,
                deadline: Instant::now() + Duration::from_secs(10),
            }
        )
        .await
        .is_none());
        assert!(register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            2,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner,
                expected_keys: vec!["key".to_string()],
                origin_tx: origin,
                origin_req_id: 12,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: gate.try_enter().unwrap(),
            }
        )
        .await
        .is_none());
        assert!(reads.lock().unwrap().is_empty());
        assert!(mutations.lock().unwrap().is_empty());
        let response = origin_responses.try_recv().unwrap();
        assert!(matches!(Envelope::decode(response.bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 12 && response.results[0].status == MutationStatus::Unavailable as i32));
        assert_eq!(admission.slots.available_permits(), 2);
        assert_eq!(admission.key_bytes.available_permits(), 1024);
        gate.begin_shutdown();
        gate.wait_for_drain().await;
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn dropping_relay_registration_before_timer_ack_reclaims_both_types() {
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let admission = RelayAdmission::new(2, 1024);
        let reads: RelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (owner, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_response_rx) = test_peer_channel(1);
        let gate = OperationGate::new();
        // Current-thread Tokio cannot run its manager between this one poll
        // and Drop: both registry insertion and queued timer registration have
        // happened, but the timer acknowledgment has not.
        let mut read = Box::pin(register_get_relay(
            &task_ref,
            reads.clone(),
            &admission,
            1,
            GetRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner.clone(),
                expected_key: "key".to_string(),
                origin_tx: origin.clone(),
                origin_req_id: 11,
                operation: None,
                deadline: Instant::now() + Duration::from_secs(10),
            },
        ));
        assert!(futures::poll!(read.as_mut()).is_pending());
        assert_eq!(reads.lock().unwrap().len(), 1);
        drop(read);
        assert!(reads.lock().unwrap().is_empty());
        assert_eq!(
            admission.slots.available_permits(),
            1,
            "queued timer lost its admission lease"
        );
        let mut mutation = Box::pin(register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            2,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner,
                expected_keys: vec!["key".to_string()],
                origin_tx: origin,
                origin_req_id: 12,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: gate.try_enter().unwrap(),
            },
        ));
        assert!(futures::poll!(mutation.as_mut()).is_pending());
        assert_eq!(mutations.lock().unwrap().len(), 1);
        drop(mutation);
        assert!(mutations.lock().unwrap().is_empty());
        let response = origin_response_rx
            .try_recv()
            .expect("canceled mutation registration must reply unavailable");
        assert!(matches!(Envelope::decode(response.bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 12 && response.results[0].status == MutationStatus::Unavailable as i32));
        gate.begin_shutdown();
        gate.wait_for_drain().await;
        wait_for_relay_admission(&admission, 2, 1024).await;
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn relay_enqueue_cancellation_and_registration_failure_release_admission() {
        let tasks = RuntimeTasks::new(4);
        let task_ref = Arc::downgrade(&tasks);
        let admission = RelayAdmission::new(1, 1024);
        let mutations: MutationRelayMap = Arc::new(StdMutex::new(HashMap::new()));
        let (owner, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        let (origin, _origin_rx, mut origin_response_rx) = test_peer_channel(4);
        let gate = OperationGate::new();
        let envelope = Envelope {
            msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                req_id: 1,
                keys: vec![],
                priority: 0,
            })),
        };
        assert!(
            try_send_envelope_until(
                &owner,
                envelope.clone(),
                Instant::now() + Duration::from_secs(1)
            )
            .await
        );
        let registration = register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            1,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner.clone(),
                expected_keys: vec!["key".to_string()],
                origin_tx: origin.clone(),
                origin_req_id: 11,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: gate.try_enter().unwrap(),
            },
        )
        .await
        .unwrap();
        let mut enqueue = Box::pin(async move {
            assert!(
                registration
                    .enqueue(&owner, envelope, Instant::now() + Duration::from_secs(10))
                    .await
            );
            registration.disarm();
        });
        assert!(futures::poll!(enqueue.as_mut()).is_pending());
        drop(enqueue);
        assert!(mutations.lock().unwrap().is_empty());
        let response = origin_response_rx.try_recv().unwrap();
        assert!(matches!(Envelope::decode(response.bytes()).unwrap().msg,
            Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 11 && response.results[0].status == MutationStatus::Unavailable as i32));
        wait_for_relay_admission(&admission, 1, 1024).await;
        // A stopped task owner rejects registration; no map/timer/operation
        // may remain and the downstream frame must never be enqueued.
        tasks.shutdown().await;
        let (owner, _owner_rx, _owner_response_rx) = test_peer_channel(1);
        assert!(register_mutation_relay(
            &task_ref,
            mutations.clone(),
            &admission,
            2,
            MutationRelayRegistration {
                expected_peer: "owner".to_string(),
                expected_tx: owner,
                expected_keys: vec!["key".to_string()],
                origin_tx: origin,
                origin_req_id: 12,
                deadline: Instant::now() + Duration::from_secs(10),
                operation: gate.try_enter().unwrap(),
            }
        )
        .await
        .is_none());
        assert!(mutations.lock().unwrap().is_empty());
        assert!(origin_response_rx.try_recv().is_ok());
        wait_for_relay_admission(&admission, 1, 1024).await;
        gate.begin_shutdown();
        gate.wait_for_drain().await;
    }

    #[tokio::test]
    async fn batch_relay_preserves_duplicate_positions_stamps_and_terminal_capacity() {
        let (mut ctx, tasks) = test_connection_context(
            FragmentRouter::new(1, 0),
            Arc::new(RwLock::new(HashSet::new())),
            Arc::new(RwLock::new(HashMap::from([(
                FragmentId(0),
                vec!["owner".to_string()],
            )]))),
            true,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        )
        .await;
        ctx.relay_admission = RelayAdmission::new(1, 1024);
        ctx.runtime_options = ctx
            .runtime_options
            .with_forwarded_attempt_timeout(Duration::from_secs(10));
        let (mut owner, owner_server) = tokio::io::duplex(4096);
        let owner_connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(owner_server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut owner, "owner").await;
        // This response proves the owner handshake published its exact channel
        // before the origin requests a relay through it.
        write_test_envelope(
            &mut owner,
            Envelope {
                msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                    req_id: 300,
                    ..Default::default()
                })),
            },
        )
        .await;
        assert!(matches!(read_test_envelope(&mut owner).await.msg,
            Some(EnvelopeMsg::BatchGetResponse(response)) if response.req_id == 300));
        let (mut origin, origin_server) = tokio::io::duplex(4096);
        let origin_connection = tokio::spawn(handle_connection(
            ctx.clone(),
            Box::pin(origin_server),
            false,
            None,
            None,
        ));
        complete_test_handshake(&mut origin, "origin").await;
        let request = BatchPut {
            req_id: 400,
            priority: ProtoPriority::High as i32,
            puts: ["duplicate", "duplicate", "other"]
                .into_iter()
                .enumerate()
                .map(|(index, key)| ForwardedPut {
                    key: key.into(),
                    value: if index == 2 {
                        Vec::new()
                    } else {
                        vec![index as u8]
                    },
                    timestamp_ms: index as u64 + 1,
                    logical_counter: 0,
                    node_id: format!("origin-{index}"),
                    seq: index as u64 + 11,
                    boot_id: index as u64 + 21,
                    expires_at_ms: index as u64 * 10,
                    is_tombstone: index == 2,
                    priority: ProtoPriority::Background as i32,
                    hops: index as u32,
                    req_id: index as u64 + 500,

                    write_concern: None,
                })
                .collect(),
        };
        write_test_envelope(
            &mut origin,
            Envelope {
                msg: Some(EnvelopeMsg::BatchPut(request.clone())),
            },
        )
        .await;
        let Some(EnvelopeMsg::BatchPut(forwarded)) = read_test_envelope(&mut owner).await.msg
        else {
            panic!("owner did not receive the forwarded batch");
        };
        assert_ne!(forwarded.req_id, request.req_id);
        let mut expected = request.clone();
        expected.req_id = forwarded.req_id;
        for put in &mut expected.puts {
            put.hops += 1;
        }
        assert_eq!(
            forwarded, expected,
            "relay changed operation stamps or positions"
        );
        assert_eq!(ctx.pending_mutation_relays.lock().unwrap().len(), 1);
        assert_eq!(ctx.relay_admission.slots.available_permits(), 0);
        assert!(ctx.relay_admission.key_bytes.available_permits() < 1024);
        ctx.operation_gate.begin_shutdown();
        let mut drain = Box::pin(ctx.operation_gate.wait_for_drain());
        assert!(futures::poll!(drain.as_mut()).is_pending());

        let mut superseded = result("duplicate", 1);
        superseded.status = MutationStatus::Superseded as i32;
        superseded.winner_timestamp_ms = 4;
        superseded.winner_node_id = "winner".into();
        superseded.winner_is_tombstone = true;
        let mut unavailable = result("other", 2);
        unavailable.status = MutationStatus::Unavailable as i32;
        unavailable.error = "downstream unavailable".into();
        let results = vec![result("duplicate", 0), superseded, unavailable];
        write_test_envelope(
            &mut owner,
            Envelope {
                msg: Some(EnvelopeMsg::MutationResponse(MutationResponse {
                    req_id: forwarded.req_id,
                    results: results.clone(),
                })),
            },
        )
        .await;
        assert_eq!(
            read_test_envelope(&mut origin).await.msg,
            Some(EnvelopeMsg::MutationResponse(MutationResponse {
                req_id: request.req_id,
                results,
            })),
            "relay changed duplicate positions or downstream statuses",
        );
        wait_for_relay_admission(&ctx.relay_admission, 1, 1024).await;
        assert!(ctx.pending_mutation_relays.lock().unwrap().is_empty());
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .expect("terminal batch response retained the operation fence");
        drop(origin);
        drop(owner);
        for connection in [origin_connection, owner_connection] {
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .unwrap()
                .unwrap();
        }
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn silent_owner_ingress_is_bounded_by_shared_relay_count_and_key_bytes() {
        for bound_by_bytes in [false, true] {
            let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
            let (owner, mut owner_rx, _owner_response_rx) = test_peer_channel(8);
            peers.lock().await.insert("owner".to_string(), owner);
            let (mut ctx, tasks) = test_connection_context(
                FragmentRouter::new(1, 0),
                Arc::new(RwLock::new(HashSet::new())),
                Arc::new(RwLock::new(HashMap::from([(
                    FragmentId(0),
                    vec!["owner".to_string()],
                )]))),
                true,
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                peers,
            )
            .await;
            let slots = if bound_by_bytes { 8 } else { 2 };
            let bytes = if bound_by_bytes {
                relay_key_storage_bytes(3) + relay_keys_storage_bytes(&["key".to_string()], 1)
            } else {
                1024
            };
            ctx.relay_admission = RelayAdmission::new(slots, bytes);
            ctx.runtime_options = ctx
                .runtime_options
                .with_forwarded_attempt_timeout(Duration::from_millis(500));
            let admission = ctx.relay_admission.clone();
            let reads = ctx.pending_relays.clone();
            let mutations = ctx.pending_mutation_relays.clone();
            let (mut client, server) = tokio::io::duplex(4096);
            let connection =
                tokio::spawn(handle_connection(ctx, Box::pin(server), false, None, None));
            complete_test_handshake(&mut client, "origin").await;
            let read = |req_id| Envelope {
                msg: Some(EnvelopeMsg::ForwardedGet(ForwardedGet {
                    key: "key".to_string(),
                    req_id,
                    priority: ProtoPriority::High as i32,
                    hops: 0,
                })),
            };
            let put = |req_id| Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
                    key: "key".to_string(),
                    value: b"value".to_vec(),
                    timestamp_ms: 1,
                    logical_counter: 0,
                    node_id: "origin".to_string(),
                    seq: req_id,
                    boot_id: 1,
                    expires_at_ms: 0,
                    is_tombstone: false,
                    priority: ProtoPriority::Default as i32,
                    hops: 0,
                    req_id,

                    write_concern: None,
                })),
            };
            write_test_envelope(&mut client, read(10)).await;
            write_test_envelope(&mut client, put(11)).await;
            let forwarded_read = tokio::time::timeout(Duration::from_secs(1), owner_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(Envelope::decode(forwarded_read.bytes()).unwrap().msg,
                Some(EnvelopeMsg::ForwardedGet(g)) if g.req_id != 10 && g.priority == ProtoPriority::High as i32 && g.hops == 1)
            );
            let forwarded_put = tokio::time::timeout(Duration::from_secs(1), owner_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(Envelope::decode(forwarded_put.bytes()).unwrap().msg,
                Some(EnvelopeMsg::ForwardedPut(p)) if p.req_id != 11 && p.timestamp_ms == 1 && p.seq == 11 && p.hops == 1)
            );
            for req_id in 20..40 {
                write_test_envelope(&mut client, read(req_id)).await;
                write_test_envelope(&mut client, put(req_id + 100)).await;
                assert!(matches!(read_test_envelope(&mut client).await.msg,
                    Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == req_id + 100 && response.results[0].status == MutationStatus::Unavailable as i32));
                assert_eq!(reads.lock().unwrap().len(), 1);
                assert_eq!(mutations.lock().unwrap().len(), 1);
            }
            assert!(
                owner_rx.try_recv().is_err(),
                "rejected relays reached the owner"
            );
            let batch_put = ForwardedPut {
                key: "key".to_string(),
                value: b"value".to_vec(),
                timestamp_ms: 2,
                logical_counter: 0,
                node_id: "origin".to_string(),
                seq: 400,
                boot_id: 1,
                req_id: 400,
                ..Default::default()
            };
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchPut(BatchPut {
                        req_id: 400,
                        puts: vec![batch_put.clone(), batch_put],
                        priority: ProtoPriority::High as i32,
                    })),
                },
            )
            .await;
            assert!(matches!(read_test_envelope(&mut client).await.msg,
                Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 400 && response.results.len() == 2
                    && response.results.iter().enumerate().all(|(index, result)| result.key == "key" && result.request_index as usize == index && result.status == MutationStatus::Unavailable as i32)));
            // Barrier proves the saturated peer reader remains usable and no
            // unavailable legacy read was converted into a false miss.
            write_test_envelope(
                &mut client,
                Envelope {
                    msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                        req_id: 300,
                        keys: vec![],
                        priority: 0,
                    })),
                },
            )
            .await;
            assert!(matches!(read_test_envelope(&mut client).await.msg,
                Some(EnvelopeMsg::BatchGetResponse(response)) if response.req_id == 300 && response.results.is_empty()));
            assert!(matches!(read_test_envelope(&mut client).await.msg,
                Some(EnvelopeMsg::MutationResponse(response)) if response.req_id == 11 && response.results[0].status == MutationStatus::Unavailable as i32));
            wait_for_relay_admission(&admission, slots, bytes).await;
            assert!(reads.lock().unwrap().is_empty());
            assert!(mutations.lock().unwrap().is_empty());
            drop(client);
            tokio::time::timeout(Duration::from_secs(1), connection)
                .await
                .unwrap()
                .unwrap();
            tasks.shutdown().await;
        }
    }

    async fn wait_for_relay_admission(admission: &RelayAdmission, slots: usize, bytes: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while admission.slots.available_permits() != slots
                || admission.key_bytes.available_permits() != bytes
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("relay admission was not reclaimed after timer exit");
    }
}
