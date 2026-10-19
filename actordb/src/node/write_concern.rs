//! Routed per-write acknowledgement waits; ordinary writes retain local completion.
use super::*;
use crate::write_concern::{
    decode_outcome, WriteConcernError, WriteConcernOptions, WriteConcernReceipt,
};

impl Node {
    pub async fn put_with_write_concern(
        &self,
        key: String,
        value: Vec<u8>,
        options: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernError> {
        self.put_with_write_concern_options(key, value, None, OpPriority::Default, options)
            .await
    }

    pub async fn put_with_write_concern_options(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
        priority: OpPriority,
        options: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernError> {
        let _operation = self.admit_operation()?;
        options.validate()?;
        let fragment = fragment_of(&key, self.router.fragment_count);
        if self.serving.read().unwrap().contains(&fragment) {
            self.write_concerns.plan(fragment, &options)?;
        }
        validate_record_wire_budget(&key, value.len(), &self.node_id)?;
        let expires_at_ms = ttl
            .map(|ttl| ttl_expiry_deadline_ms(crate::actor::now_ms(), ttl))
            .transpose()?;
        let seq = {
            let mut seq = self.next_seq.lock().await;
            *seq += 1;
            *seq
        };
        let mut value =
            VersionedValue::new(value, self.hlc.lock().await.tick(), self.node_id.clone());
        value.expires_at_ms = expires_at_ms;
        let op = ReplicatedOp {
            key,
            value,
            seq,
            boot_id: self.boot_id,
        };
        self.apply_routed_with_concern(op, priority, Some(&options))
            .await?
            .ok_or_else(|| WriteConcernError::UnknownOutcome("missing concern receipt".into()))
    }

    pub async fn delete_with_write_concern(
        &self,
        key: String,
        options: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernError> {
        self.delete_with_write_concern_options(key, OpPriority::Default, options)
            .await
    }

    pub async fn delete_with_write_concern_options(
        &self,
        key: String,
        priority: OpPriority,
        options: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernError> {
        let _operation = self.admit_operation()?;
        options.validate()?;
        let fragment = fragment_of(&key, self.router.fragment_count);
        if self.serving.read().unwrap().contains(&fragment) {
            self.write_concerns.plan(fragment, &options)?;
        }
        validate_record_wire_budget(&key, 0, &self.node_id)?;
        let seq = {
            let mut seq = self.next_seq.lock().await;
            *seq += 1;
            *seq
        };
        let op = ReplicatedOp {
            key,
            value: VersionedValue::tombstone(self.hlc.lock().await.tick(), self.node_id.clone()),
            seq,
            boot_id: self.boot_id,
        };
        self.apply_routed_with_concern(op, priority, Some(&options))
            .await?
            .ok_or_else(|| WriteConcernError::UnknownOutcome("missing concern receipt".into()))
    }
    pub(super) async fn apply_routed_with_concern(
        &self,
        op: ReplicatedOp,
        priority: OpPriority,
        options: Option<&WriteConcernOptions>,
    ) -> Result<Option<WriteConcernReceipt>, WriteConcernError> {
        validate_replicated_op(&op)?;
        let policy = options.map(WriteConcernOptions::encode).transpose()?;
        let frag = fragment_of(&op.key, self.router.fragment_count);
        let mutation_lease = if self.placement_controller.is_some() {
            Some(self.router.mutation_gate(frag).read_owned().await)
        } else {
            None
        };
        let local_actor = if self.serving.read().unwrap().contains(&frag) {
            self.router.route_for_fragment(frag)
        } else {
            None
        };
        if let Some(actor) = local_actor {
            let pending = options
                .map(|options| self.write_concerns.prepare(frag, &op, options))
                .transpose()?;
            let outcome = call_apply(&actor, op.clone(), priority, false).await?;

            match outcome {
                ApplyOutcome::Applied | ApplyOutcome::AlreadyPresent => {}
                ApplyOutcome::Superseded {
                    timestamp_ms,
                    node_id,
                    is_tombstone,
                } => {
                    return Err(NodeError::Superseded {
                        timestamp_ms,
                        node_id,
                        is_tombstone,
                    }
                    .into())
                }
                ApplyOutcome::Expired => {
                    let mut expiry = op.clone();
                    expiry.value = expiry.value.expiry_tombstone();
                    self.enqueue_for_replication(frag, &expiry).await;
                    return Err(NodeError::Expired.into());
                }
            }

            self.enqueue_for_replication(frag, &op).await;
            drop(mutation_lease);
            if let Some(pending) = pending {
                pending.applied_locally();
                return pending.wait().await.map(Some);
            }
            return Ok(None);
        }
        drop(mutation_lease);

        // Remote fragment: retry the exact stamped operation across the
        // configured holders. A prior attempt may have applied the value and
        // lost its response, so every retry carries the same LWW version.
        let replicas = self.replica_order(frag);
        if replicas.is_empty() {
            return Err(NodeError::Actor(format!("no replica for fragment {frag}")).into());
        }
        let expected_key = op.key.clone();
        let mut tried = Vec::with_capacity(replicas.len());
        for owner in replicas {
            tried.push(owner.clone());
            let Some(tx) = self.peer_sender(&owner).await else {
                continue;
            };
            let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
            let (resp_tx, resp_rx) = oneshot::channel();
            self.pending_mutations.lock().unwrap().insert(
                req_id,
                PendingMutation {
                    expected_peer: owner.clone(),
                    expected_keys: vec![expected_key.clone()],
                    reply: resp_tx,
                },
            );
            let _pending_guard = PendingMutationGuard {
                req_id,
                pending: self.pending_mutations.clone(),
            };
            let wait = options.map_or(std::time::Duration::ZERO, WriteConcernOptions::timeout);
            let timeout = self
                .runtime_options
                .forwarded_attempt_timeout
                .checked_add(wait)
                .ok_or_else(|| {
                    WriteConcernError::InvalidPolicy(
                        "forwarded concern timeout exceeds range".into(),
                    )
                })?;
            let attempt_deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
                WriteConcernError::InvalidPolicy("forwarded concern timeout exceeds range".into())
            })?;
            let envelope = Envelope {
                msg: Some(EnvelopeMsg::ForwardedPut(ForwardedPut {
                    key: op.key.clone(),
                    value: op.value.value.clone(),
                    timestamp_ms: op.value.timestamp_ms,
                    node_id: op.value.node_id.clone(),
                    seq: op.seq,
                    boot_id: op.boot_id,
                    expires_at_ms: op.value.expires_at_ms.unwrap_or(0),
                    is_tombstone: op.value.is_tombstone,
                    priority: priority_to_proto(priority),
                    hops: 0,
                    req_id,

                    write_concern: policy.clone(),
                })),
            };
            check_envelope_budget(&envelope, MAX_FRAME_BYTES).map_err(|e| NodeError::Codec(e.to_string()))?;
            if !try_send_envelope_until(&tx, envelope, attempt_deadline).await {
                continue;
            }
            let response = tokio::time::timeout_at(attempt_deadline, resp_rx).await;
            let Ok(Ok(mut results)) = response else {
                continue;
            };
            if validate_mutation_results(std::slice::from_ref(&expected_key), &results).is_err() {
                continue;
            }
            let result = results.remove(0);
            if MutationStatus::try_from(result.status) == Ok(MutationStatus::Unavailable) {
                continue;
            }
            if let Some(options) = options {
                if let Some(bytes) = result.write_concern.as_deref() {
                    return decode_outcome(bytes, options).map(Some);
                }
                decode_mutation_result(result)?;
                return Err(WriteConcernError::UnknownOutcome(
                    "mutation reply omitted write concern acknowledgement".into(),
                ));
            }
            decode_mutation_result(result)?;
            return Ok(None);
        }
        Err(NodeError::Network(format!(
            "no replica acknowledged mutation for fragment {frag} (tried {})",
            tried.join(",")
        ))
        .into())
    }
}
