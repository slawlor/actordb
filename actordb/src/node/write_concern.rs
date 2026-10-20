//! Routed per-write acknowledgement waits; ordinary writes retain local completion.
use super::*;
use crate::write_concern::{
    decode_outcome, WriteConcernError, WriteConcernOptions, WriteConcernReceipt,
};

impl Node {
    /// Cap acknowledgement counts for subsequently received writes. The cap
    /// must be positive and never raises a request's threshold or removes other
    /// write requirements. Existing waiters retain their captured thresholds.
    ///
    /// This explicitly accepts weaker replication assurance during an outage;
    /// peer connectivity never changes the threshold automatically.
    pub fn set_write_concern_acknowledgement_cap(&self, cap: u32) -> Result<(), WriteConcernError> {
        self.write_concerns.set_acknowledgement_cap(cap)
    }

    /// Restore requested acknowledgement counts for subsequently received writes.
    /// Existing waiters retain the cap captured when they were prepared.
    pub fn clear_write_concern_acknowledgement_cap(&self) {
        self.write_concerns.clear_acknowledgement_cap();
    }

    /// Current operational cap used when this node prepares a new write plan.
    pub fn write_concern_acknowledgement_cap(&self) -> Option<u32> {
        self.write_concerns.acknowledgement_cap()
    }

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
        let retention =
            self.embedded_operations
                .admit(embedded_operations::single_request_bytes(
                    &key,
                    Some(&value),
                ))?;
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
        let mut value = VersionedValue::new_version(
            value,
            self.hlc
                .lock()
                .await
                .tick()
                .map_err(|error| NodeError::Codec(error.to_string()))?,
            self.node_id.clone(),
        );
        value.expires_at_ms = expires_at_ms;
        let op = ReplicatedOp {
            key,
            value,
            seq,
            boot_id: self.boot_id,
        };
        self.apply_routed_with_concern(op, priority, Some(&options), retention)
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
        let retention = self
            .embedded_operations
            .admit(embedded_operations::single_request_bytes(&key, None))?;
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
            value: VersionedValue::tombstone_version(
                self.hlc
                    .lock()
                    .await
                    .tick()
                    .map_err(|error| NodeError::Codec(error.to_string()))?,
                self.node_id.clone(),
            ),
            seq,
            boot_id: self.boot_id,
        };
        self.apply_routed_with_concern(op, priority, Some(&options), retention)
            .await?
            .ok_or_else(|| WriteConcernError::UnknownOutcome("missing concern receipt".into()))
    }
    pub(super) async fn apply_routed_with_concern(
        &self,
        op: ReplicatedOp,
        priority: OpPriority,
        options: Option<&WriteConcernOptions>,
        retention: Arc<RequestRetention>,
    ) -> Result<Option<WriteConcernReceipt>, WriteConcernError> {
        validate_replicated_op(&op)?;
        let policy = options.map(WriteConcernOptions::encode).transpose()?;
        let frag = fragment_of(&op.key, self.router.fragment_count);
        let mutation_lease = if self.placement_controller.is_some() {
            Some(
                tokio::time::timeout(
                    self.runtime_options.local_work_timeout(),
                    self.router.mutation_gate(frag).read_owned(),
                )
                .await
                .map_err(|_| {
                    NodeError::Actor(
                        "mutation authority wait timed out before apply admission".into(),
                    )
                })?,
            )
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
            let operation = self.admit_operation()?;
            let senders = self.senders.clone();
            let (reply, response) = oneshot::channel();
            let budget = self
                .runtime_options
                .local_work_timeout()
                .checked_add(options.map_or(Duration::ZERO, WriteConcernOptions::timeout))
                .ok_or_else(|| NodeError::Codec("local mutation deadline overflow".into()))?;
            self.embedded_operations.submit(async move {
                // These leases belong to actual completion, independently of
                // the caller waiter. No accepted ApplyOp is abandoned at its
                // user deadline; exact outcome and fanout must finish first.
                let _retention = retention;
                let _operation = operation;
                let result = async {
                    let outcome = call_apply(&actor, op.clone(), priority, false).await?;
                    match outcome {
                        ApplyOutcome::Applied | ApplyOutcome::AlreadyPresent => {}
                        ApplyOutcome::Superseded {
                            timestamp_ms,
                            logical_counter,
                            node_id,
                            is_tombstone,
                        } => {
                            return Err(NodeError::Superseded {
                                timestamp_ms,
                                logical_counter,
                                node_id,
                                is_tombstone,
                            }
                            .into());
                        }
                        ApplyOutcome::Expired => {
                            let mut expiry = op;
                            expiry.value = expiry.value.expiry_tombstone();
                            enqueue_to_senders(&senders, frag, &expiry).await;
                            return Err(NodeError::Expired.into());
                        }
                    }
                    enqueue_to_senders(&senders, frag, &op).await;
                    drop(mutation_lease);
                    if let Some(pending) = pending {
                        pending.applied_locally();
                        pending.wait().await.map(Some)
                    } else {
                        Ok(None)
                    }
                }
                .await;
                let _ = reply.send(result);
            })?;
            return tokio::time::timeout(budget, response)
                .await
                .map_err(|_| {
                    WriteConcernError::UnknownOutcome(
                        "local mutation waiter deadline exceeded; completion remains owned".into(),
                    )
                })?
                .map_err(|_| {
                    WriteConcernError::UnknownOutcome(
                        "local mutation completion owner lost its reply".into(),
                    )
                })?;
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
        let mut ambiguous_attempt = false;
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
                    logical_counter: op.value.logical_counter,
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
            check_envelope_budget(&envelope, MAX_FRAME_BYTES)
                .map_err(|e| NodeError::Codec(e.to_string()))?;
            if !try_send_envelope_until(&tx, envelope, attempt_deadline).await {
                continue;
            }
            let response = tokio::time::timeout_at(attempt_deadline, resp_rx).await;
            let Ok(Ok(mut results)) = response else {
                ambiguous_attempt = true;
                continue;
            };
            if validate_mutation_results(std::slice::from_ref(&expected_key), &results).is_err() {
                ambiguous_attempt = true;
                continue;
            }
            let result = results.remove(0);
            if MutationStatus::try_from(result.status) == Ok(MutationStatus::Unavailable) {
                ambiguous_attempt = true;
                continue;
            }
            if let Some(options) = options {
                if let Some(bytes) = result.write_concern.as_deref() {
                    return resolve_concern_attempt(
                        decode_forwarded_concern(&result, bytes, options),
                        ambiguous_attempt,
                    )
                    .map(Some);
                }
                if let Err(error) = decode_mutation_result(result) {
                    return resolve_concern_attempt(Err(error.into()), ambiguous_attempt).map(Some);
                }
                return Err(WriteConcernError::UnknownOutcome(
                    "mutation reply omitted write concern acknowledgement".into(),
                ));
            }
            if let Err(error) = decode_mutation_result(result) {
                if ambiguous_attempt {
                    return Err(WriteConcernError::UnknownOutcome(format!(
                        "an earlier forwarded mutation may have applied; latest attempt returned: {error}"
                    )));
                }
                return Err(error.into());
            }
            return Ok(None);
        }
        if ambiguous_attempt {
            return Err(WriteConcernError::UnknownOutcome(format!(
                "forwarded mutation may have applied; no conclusive reply for fragment {frag} (tried {})",
                tried.join(",")
            )));
        }
        Err(NodeError::Network(format!(
            "no replica acknowledged mutation for fragment {frag} (tried {})",
            tried.join(",")
        ))
        .into())
    }
}

fn resolve_concern_attempt(
    outcome: Result<WriteConcernReceipt, WriteConcernError>,
    ambiguous_attempt: bool,
) -> Result<WriteConcernReceipt, WriteConcernError> {
    match outcome {
        Err(error)
            if ambiguous_attempt
                && !matches!(
                    error,
                    WriteConcernError::TimedOut(_) | WriteConcernError::UnknownOutcome(_)
                ) =>
        {
            Err(WriteConcernError::UnknownOutcome(format!(
                "an earlier forwarded mutation may have applied; latest attempt returned: {error}"
            )))
        }
        outcome => outcome,
    }
}

fn decode_forwarded_concern(
    result: &MutationResult,
    bytes: &[u8],
    options: &WriteConcernOptions,
) -> Result<WriteConcernReceipt, WriteConcernError> {
    let outcome = decode_outcome(bytes, options);
    if (outcome.is_ok() || matches!(outcome, Err(WriteConcernError::TimedOut(_))))
        && !matches!(
            MutationStatus::try_from(result.status),
            Ok(MutationStatus::Applied | MutationStatus::AlreadyPresent)
        )
    {
        return Err(WriteConcernError::UnknownOutcome(
            "write concern progress conflicts with mutation status".into(),
        ));
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_concern::WriteConcernOutcome;

    #[test]
    fn forwarded_receipt_requires_local_application_status() {
        let options = WriteConcernOptions::builder()
            .concern(crate::write_concern::WriteConcern::Count(2))
            .build();
        let receipt =
            WriteConcernReceipt::new("owner".into(), 3, 2, 2, vec!["owner".into(), "peer".into()])
                .unwrap();
        let partial =
            WriteConcernReceipt::new("owner".into(), 3, 2, 2, vec!["owner".into()]).unwrap();
        for concern in [
            WriteConcernOutcome::Satisfied(receipt),
            WriteConcernOutcome::TimedOut(partial),
        ] {
            let bytes = concern.encode();
            for status in [
                MutationStatus::Applied,
                MutationStatus::AlreadyPresent,
                MutationStatus::Expired,
                MutationStatus::Superseded,
                MutationStatus::StorageError,
                MutationStatus::ActorError,
                MutationStatus::Backpressure,
            ] {
                let mut result = MutationResult {
                    key: "key".into(),
                    status: status as i32,
                    write_concern: Some(bytes.clone()),
                    ..Default::default()
                };
                if status == MutationStatus::Superseded {
                    result.winner_node_id = "winner".into();
                }
                validate_mutation_results(&["key".into()], std::slice::from_ref(&result)).unwrap();
                let decoded = decode_forwarded_concern(&result, &bytes, &options);
                if matches!(
                    status,
                    MutationStatus::Applied | MutationStatus::AlreadyPresent
                ) {
                    match concern {
                        WriteConcernOutcome::Satisfied(_) => assert!(decoded.is_ok()),
                        WriteConcernOutcome::TimedOut(_) => {
                            assert!(matches!(decoded, Err(WriteConcernError::TimedOut(_))));
                        }
                        _ => unreachable!(),
                    }
                } else {
                    assert!(matches!(decoded, Err(WriteConcernError::UnknownOutcome(_))));
                }
            }
        }
    }
}
