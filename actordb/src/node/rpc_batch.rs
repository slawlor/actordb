//! Bounded application batching over existing actor/peer page machinery.
//! One read page is active at a time; writes batch independent keys in ordered waves.
use super::*;
use crate::query::QueryRetention;
use crate::rpc::{error, mutation_result as rpc_mutation_result, read_result, BatchReply};
use actordb_protocol::{client as wire, MAX_RPC_MESSAGE_BYTES};
use std::collections::BTreeMap;

const READ_PAGE_KEYS: usize = 256;
const READ_RUNTIME_BYTES: usize = 4 * MAX_RPC_MESSAGE_BYTES;
const PUT_WAVE_KEYS: usize = 128;

impl Node {
    pub(crate) async fn rpc_batch_get(
        &self,
        table: &str,
        keys: Vec<String>,
        priority: OpPriority,
    ) -> Result<(Vec<wire::ReadResult>, Option<Arc<QueryRetention>>), wire::Error> {
        if keys.is_empty() {
            return Ok((Vec::new(), None));
        }
        let count = keys.len();
        let mut reply = BatchReply::new(count, 2);
        let operation = match self.admit_table_operation(table) {
            Ok(operation) => operation,
            Err(_) => return rejected_reads(keys, NodeError::ShuttingDown),
        };
        // Request strings/indices/slots plus a constant allowance for a final
        // reply, an active bounded actor/peer page and validation/key copies.
        // This lease moves into the RPC reply and follows local actor work
        // after cancellation; never collect an unbounded embedded get_multi.
        let request_bytes = keys
            .capacity()
            .saturating_mul(std::mem::size_of::<String>())
            .saturating_add(keys.iter().map(String::capacity).sum::<usize>())
            .saturating_add(count.saturating_mul(256))
            .saturating_add(4096);
        let retention = match self.query_admission.reserve_work_retained(
            request_bytes,
            READ_RUNTIME_BYTES,
            Some(Arc::new(operation)),
        ) {
            Ok(retention) => retention,
            Err(error) => return rejected_reads(keys, NodeError::Backpressure(error.to_string())),
        };
        let mut groups = BTreeMap::<FragmentId, Vec<usize>>::new();
        for (index, key) in keys.iter().enumerate() {
            groups
                .entry(crate::shard::fragment_of_literal(
                    key,
                    self.router.fragment_count,
                ))
                .or_default()
                .push(index);
        }
        let mut keys: Vec<_> = keys.into_iter().map(Some).collect();
        for (fragment, indices) in groups {
            let mut cursor = 0;
            while cursor < indices.len() {
                // Qualify only one request page, not every input key or value.
                let begin = cursor;
                let mut payload = batch_get_base_encoded_len(priority_to_proto(priority));
                while cursor < indices.len() && cursor - begin < READ_PAGE_KEYS {
                    let key = crate::table_key::encode_table_key(
                        table,
                        keys[indices[cursor]].as_ref().unwrap(),
                    );
                    let next = payload.saturating_add(message_field_len(2, key.len()));
                    if cursor != begin
                        && batch_get_envelope_encoded_len(next) > WIRE_BATCH_PAGE_BYTES
                    {
                        break;
                    }
                    payload = next;
                    cursor += 1;
                }
                let page_indices = &indices[begin..cursor];
                let local = self
                    .router
                    .route_with_generation(fragment)
                    .filter(|_| self.serving.read().unwrap().contains(&fragment));
                let remote = self
                    .replica_order(fragment)
                    .into_iter()
                    .filter(|owner| owner != &self.node_id)
                    .collect::<Vec<_>>();
                let mut offset = 0;
                if let Some((generation, actor)) = local {
                    let deadline = deadline_after(self.runtime_options.local_work_timeout());
                    while offset < page_indices.len() {
                        let requested = qualify(table, &keys, &page_indices[offset..]);
                        let ownership = retention.clone();
                        let page = tokio::time::timeout_at(
                            deadline,
                            actor.call(
                                move |reply| {
                                    KvMessage::GetBatchRetained(
                                        crate::actor::RetainedBatchRead::new(
                                            requested,
                                            0,
                                            WIRE_BATCH_PAGE_BYTES,
                                            reply,
                                            priority,
                                            ownership,
                                        ),
                                    )
                                },
                                None,
                            ),
                        )
                        .await;
                        let Ok(Ok(CallResult::Success(page))) = page else {
                            break;
                        };
                        if !self.serving.read().unwrap().contains(&fragment)
                            || !self.router.is_current(fragment, generation, &actor)
                            || page.entries.is_empty()
                            || page.entries.len() > page_indices.len() - offset
                            || page.entries.iter().zip(&page_indices[offset..]).any(
                                |(entry, index)| {
                                    entry.key
                                        != crate::table_key::encode_table_key(
                                            table,
                                            keys[*index].as_ref().unwrap(),
                                        )
                                },
                            )
                        {
                            break;
                        }
                        let progress = page.entries.len();
                        for (entry, index) in page.entries.into_iter().zip(&page_indices[offset..])
                        {
                            let result = entry.result.map_err(NodeError::Backpressure);
                            reply.insert(
                                *index,
                                read_result(keys[*index].take().unwrap(), result),
                            )?;
                        }
                        offset += progress;
                    }
                }
                for owner in &remote {
                    let Some(tx) = self.peer_sender(owner).await else {
                        continue;
                    };
                    let deadline = deadline_after(self.runtime_options.forwarded_attempt_timeout);
                    while offset < page_indices.len() && Instant::now() < deadline {
                        let requested = qualify(table, &keys, &page_indices[offset..]);
                        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
                        let envelope = Envelope {
                            msg: Some(EnvelopeMsg::BatchGet(BatchGet {
                                req_id,
                                keys: requested.clone(),
                                priority: priority_to_proto(priority),
                            })),
                        };
                        if check_envelope_budget(&envelope, MAX_FRAME_BYTES).is_err() {
                            let index = page_indices[offset];
                            reply.insert(
                                index,
                                read_result(
                                    keys[index].take().unwrap(),
                                    Err(NodeError::Codec(
                                        "batch key exceeds peer request limit".into(),
                                    )),
                                ),
                            )?;
                            offset += 1;
                            continue;
                        }
                        let (send, receive) = oneshot::channel();
                        self.pending_batch_gets.lock().unwrap().insert(
                            req_id,
                            PendingBatchGet {
                                expected_peer: owner.clone(),
                                expected_keys: requested,
                                reply: send,
                            },
                        );
                        let pending = PendingBatchGetGuard {
                            req_id,
                            pending: self.pending_batch_gets.clone(),
                        };
                        if !try_send_envelope_until(&tx, envelope, deadline).await {
                            break;
                        }
                        let page = tokio::time::timeout_at(deadline, receive).await;
                        drop(pending);
                        let Ok(Ok(Ok(page))) = page else {
                            break;
                        };
                        if page.is_empty() {
                            break;
                        }
                        // The connection reader validated this complete prefix
                        // against the expected peer and ordered qualified keys.
                        let progress = page.len();
                        for (value, index) in page.into_iter().zip(&page_indices[offset..]) {
                            reply.insert(
                                *index,
                                read_result(keys[*index].take().unwrap(), decode_read(value)),
                            )?;
                        }
                        offset += progress;
                    }
                }
                for index in &page_indices[offset..] {
                    let failure = if remote.is_empty() {
                        NodeError::Actor(format!(
                            "no available local replica for fragment {fragment}"
                        ))
                    } else {
                        NodeError::Network(format!(
                            "no replica answered batch get for fragment {fragment}"
                        ))
                    };
                    reply.insert(
                        *index,
                        read_result(keys[*index].take().unwrap(), Err(failure)),
                    )?;
                }
            }
        }
        Ok((reply.finish(), Some(retention)))
    }

    pub(crate) async fn rpc_batch_put(
        &self,
        table: &str,
        puts: Vec<wire::Put>,
        priority: OpPriority,
    ) -> Result<Vec<wire::MutationResult>, wire::Error> {
        let mut reply = BatchReply::new(puts.len(), 3);
        let mut remaining: VecDeque<_> = puts.into();
        let mut position = 0;
        let mut overflow = false;
        while !remaining.is_empty() {
            let mut seen = HashSet::new();
            let mut values = Vec::with_capacity(PUT_WAVE_KEYS);
            let mut user_keys = Vec::with_capacity(PUT_WAVE_KEYS);
            let mut bytes = std::mem::size_of::<Vec<()>>()
                + PUT_WAVE_KEYS * std::mem::size_of::<(String, Vec<u8>, Option<Duration>)>();
            while values.len() < PUT_WAVE_KEYS {
                let Some(put) = remaining.front() else {
                    break;
                };
                // Never schedule two occurrences of one key in the same wave:
                // each later occurrence is stamped only after its predecessor
                // finishes, including its TTL and exact per-position outcome.
                if seen.contains(&put.key) {
                    break;
                }
                let key = crate::table_key::encode_table_key(table, &put.key);
                let cost = embedded_operations::single_request_bytes(&key, Some(&put.value));
                if !values.is_empty()
                    && bytes.saturating_add(cost)
                        > self.runtime_options.embedded_operation_byte_capacity()
                {
                    break;
                }
                bytes = bytes.saturating_add(cost);
                let put = remaining.pop_front().unwrap();
                seen.insert(put.key.clone());
                user_keys.push(put.key);
                values.push((key, put.value, put.ttl_ms.map(Duration::from_millis)));
            }
            drop(seen);
            let results = self
                .put_multi_qualified_with_options(values, priority)
                .await;
            for (key, result) in user_keys.into_iter().zip(results) {
                if !overflow
                    && reply
                        .insert(position, rpc_mutation_result(key, result))
                        .is_err()
                {
                    // Keep executing accepted mutation waves even if their
                    // complete reply cannot fit; never abandon apply/fan-out.
                    overflow = true;
                }
                position += 1;
            }
        }
        if overflow {
            Err(error(
                wire::ErrorCode::UnknownOutcome,
                "complete batch mutation reply exceeds RPC byte limit",
            ))
        } else {
            Ok(reply.finish())
        }
    }
}

fn qualify(table: &str, keys: &[Option<String>], indices: &[usize]) -> Vec<String> {
    indices
        .iter()
        .map(|index| crate::table_key::encode_table_key(table, keys[*index].as_ref().unwrap()))
        .collect()
}
fn decode_read(value: BatchKeyValue) -> Result<Option<VersionedValue>, NodeError> {
    if value.shed {
        return Err(NodeError::Backpressure("batch read shed by remote".into()));
    }
    if !value.found {
        return Ok(None);
    }
    let mut record = VersionedValue::new_version(
        value.value,
        actordb_protocol::RecordVersion::new(value.timestamp_ms, value.logical_counter),
        value.node_id,
    );
    record.expires_at_ms = (value.expires_at_ms != 0).then_some(value.expires_at_ms);
    record.is_tombstone = value.is_tombstone;
    Ok((!record.is_tombstone).then_some(record))
}
fn rejected_reads(
    keys: Vec<String>,
    failure: NodeError,
) -> Result<(Vec<wire::ReadResult>, Option<Arc<QueryRetention>>), wire::Error> {
    let mut reply = BatchReply::new(keys.len(), 2);
    for (index, key) in keys.into_iter().enumerate() {
        let failure = match &failure {
            NodeError::ShuttingDown => NodeError::ShuttingDown,
            _ => NodeError::Backpressure(failure.to_string()),
        };
        reply.insert(index, read_result(key, Err(failure)))?;
    }
    Ok((reply.finish(), None))
}

#[cfg(test)]
#[path = "rpc_batch_tests.rs"]
mod tests;
