// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Receiver session correlation and bounded reconciliation responses.
//!
//! Session/round generations fence stale pages. Session state is protected by
//! the inbound-session mutex; actor snapshots and response queue admission run
//! outside global session/peer locks. The serving mutation lease fences the
//! authoritative read against placement revocation.

use super::*;

pub(super) struct ServedReconcileState {
    pub(super) session_id: u64,
    pub(super) round_id: u64,
    pub(super) next_page_seq: u64,
    pub(super) next_after_key: String,
    pub(super) snapshot_revision: u64,
    pub(super) snapshot_time_ms: u64,
    pub(super) next_fetch_chunk_seq: u64,
    pub(super) advertised_keys: HashSet<String>,
    /// Subtree incarnation which produced the summaries/fetch authorization.
    /// Dispatcher restarts preserve this generation because they share state;
    /// destructive placement remove/re-add cycles allocate a new one.
    pub(super) route_generation: u64,
    pub(super) recovery_epoch: Vec<u8>,
    pub(super) recovery_replica_digest: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InboundSession {
    pub(super) session_id: u64,
    pub(super) generation: u64,
}

pub(super) type InboundSessions = Arc<Mutex<HashMap<(String, FragmentId), InboundSession>>>;

pub(super) struct ReconcileResponseJob {
    pub(super) retention: Option<Arc<dyn Send + Sync>>,
    pub(super) peer: String,
    pub(super) session: InboundSession,
    pub(super) actor: ActorRef<KvMessage>,
    pub(super) fetch: ReconcileFetch,
    pub(super) snapshot_time_ms: u64,
}

/// Keep the largest ordered summary prefix which obeys the soft page target,
/// allowing one individually valid summary to exceed it up to the hard frame
/// limit. Cursor replacement makes prefix sizes non-monotonic (a later key can
/// be shorter), so every prefix is evaluated once instead of stopping at the
/// first overflow or repeatedly rescanning while popping a suffix.
pub(super) fn fit_reconcile_response_page(
    response: &mut ReconcileResponse,
    after_key: &str,
    source_done: bool,
) -> Result<usize, usize> {
    let original_len = response.summaries.len();
    let base_payload_len = ReconcileResponse {
        fragment: response.fragment,
        summaries: Vec::new(),
        session_id: response.session_id,
        round_id: response.round_id,
        page_seq: response.page_seq,
        next_after_key: String::new(),
        snapshot_revision: response.snapshot_revision,
        snapshot_time_ms: response.snapshot_time_ms,
        done: false,
        restart: response.restart,
    }
    .encoded_len();

    if original_len == 0 {
        response.next_after_key = after_key.to_string();
        response.done = source_done;
        let payload_len = base_payload_len
            .saturating_add(bytes_field_len(6, response.next_after_key.len()))
            .saturating_add(varint_field_len(9, u64::from(response.done)));
        let encoded_len = message_field_len(12, payload_len);
        return if encoded_len <= MAX_FRAME_BYTES {
            Ok(encoded_len)
        } else {
            Err(encoded_len)
        };
    }

    let mut summaries_payload_len = 0usize;
    let mut singleton_encoded_len = 0usize;
    let mut largest_fitting = None;
    for (index, summary) in response.summaries.iter().enumerate() {
        summaries_payload_len =
            summaries_payload_len.saturating_add(message_field_len(2, summary.encoded_len()));
        let count = index + 1;
        let done = source_done && count == original_len;
        let payload_len = base_payload_len
            .saturating_add(summaries_payload_len)
            .saturating_add(bytes_field_len(6, summary.key.len()))
            .saturating_add(varint_field_len(9, u64::from(done)));
        let encoded_len = message_field_len(12, payload_len);
        if count == 1 {
            singleton_encoded_len = encoded_len;
        }
        if encoded_len <= ANTI_ENTROPY_PAGE_BYTES || (count == 1 && encoded_len <= MAX_FRAME_BYTES)
        {
            largest_fitting = Some((count, encoded_len));
        }
    }

    let Some((keep, encoded_len)) = largest_fitting else {
        return Err(singleton_encoded_len);
    };
    response.summaries.truncate(keep);
    response.next_after_key = response
        .summaries
        .last()
        .expect("a non-empty prefix was selected")
        .key
        .clone();
    response.done = source_done && keep == original_len;
    debug_assert_eq!(message_field_len(12, response.encoded_len()), encoded_len);
    Ok(encoded_len)
}

/// Payload base for a data page, conservatively reserving the terminal
/// `done=true` field before any entries are admitted.
pub(super) fn reconcile_data_reserved_base_len(page: &ReconcileData) -> usize {
    debug_assert!(!page.done);
    page.encoded_len().saturating_add(varint_field_len(7, 1))
}

/// Read an authorized fetch through actor-owned memory-bounded value pages and
/// emit exact wire-byte-bounded pages. The peer writer's byte semaphore bounds
/// encoded pages waiting on the socket.
pub(super) async fn send_reconcile_data_pages(
    actor: &ActorRef<KvMessage>,
    tx: &PeerChannel,
    fetch: ReconcileFetch,
    snapshot_time_ms: u64,
    admission_timeout: Duration,
    retention: Option<Arc<dyn Send + Sync>>,
) -> bool {
    let ReconcileFetch {
        fragment,
        session_id,
        round_id,
        chunk_seq,
        keys,
    } = fetch;
    let keys = Arc::new(keys);
    let mut next_key_index = 0usize;
    let mut page = ReconcileData {
        fragment,
        entries: Vec::new(),
        session_id,
        round_id,
        chunk_seq,
        page_seq: 0,
        done: false,
    };
    // Reserve the terminal flag on every candidate. Non-terminal pages encode
    // two bytes smaller, while the final page cannot cross the soft target
    // merely because `done=true` is set after the last value arrives.
    let mut base_payload_len = reconcile_data_reserved_base_len(&page);
    let mut sizing = RepeatedFieldSizer::new(14, 2, base_payload_len);

    while next_key_index < keys.len() {
        let request_keys = keys.clone();
        let request_retention = retention.clone();
        let value_page = match actor
            .call(
                move |reply| {
                    KvMessage::GetVersionedPage {
                        keys: request_keys,
                        start_index: next_key_index,
                        snapshot_time_ms: Some(snapshot_time_ms),
                        reply,
                    }
                    .with_optional_retention(request_retention)
                },
                None,
            )
            .await
        {
            Ok(CallResult::Success(value_page)) => value_page,
            _ => return false,
        };
        if value_page.next_index <= next_key_index || value_page.next_index > keys.len() {
            return false;
        }
        next_key_index = value_page.next_index;
        for (key, value) in value_page.entries {
            let entry = ReconcileEntry {
                key,
                value: value.value,
                timestamp_ms: value.timestamp_ms,
                logical_counter: value.logical_counter,
                node_id: value.node_id,
                expires_at_ms: value.expires_at_ms.unwrap_or(0),
                is_tombstone: value.is_tombstone,
            };

            let item_len = entry.encoded_len();
            let encoded_len = sizing.encoded_len_with(item_len);
            if encoded_len > MAX_FRAME_BYTES && page.entries.is_empty() {
                tracing::warn!(
                    fragment,
                    round_id,
                    chunk_seq,
                    encoded_len,
                    "one reconcile value exceeds the hard frame limit"
                );
                return false;
            }
            if encoded_len > MAX_FRAME_BYTES
                || (encoded_len > ANTI_ENTROPY_PAGE_BYTES && !page.entries.is_empty())
            {
                let next_page_seq = page.page_seq.saturating_add(1);
                if !try_send_envelope_with_admission_timeout(
                    tx,
                    Envelope {
                        msg: Some(EnvelopeMsg::ReconcileData(page)),
                    },
                    admission_timeout,
                )
                .await
                {
                    return false;
                }
                page = ReconcileData {
                    fragment,
                    entries: Vec::new(),
                    session_id,
                    round_id,
                    chunk_seq,
                    page_seq: next_page_seq,
                    done: false,
                };
                base_payload_len = reconcile_data_reserved_base_len(&page);
                sizing.reset(base_payload_len);
                if sizing.encoded_len_with(item_len) > MAX_FRAME_BYTES {
                    tracing::warn!(
                        fragment,
                        round_id,
                        chunk_seq,
                        encoded_len = sizing.encoded_len_with(item_len),
                        "one reconcile value exceeds the hard frame limit"
                    );
                    return false;
                }
            }
            sizing.push(item_len);
            page.entries.push(entry);
        }
    }

    page.done = true;
    try_send_envelope_with_admission_timeout(
        tx,
        Envelope {
            msg: Some(EnvelopeMsg::ReconcileData(page)),
        },
        admission_timeout,
    )
    .await
}

/// Stream fetch responses outside the connection reader. Keeping outbound
/// byte admission off that reader is essential: two peers may fetch large
/// divergent values simultaneously, and each reader must remain available to
/// drain the other peer's data while its own writer is backpressured.
pub(super) async fn run_reconcile_responder(
    mut jobs: mpsc::Receiver<ReconcileResponseJob>,
    tx: PeerChannel,
    sessions: InboundSessions,
    mut session_changes: watch::Receiver<u64>,
    admission_timeout: Duration,
) {
    let mut cancelled = tx.cancel.subscribe();
    'jobs: loop {
        if *cancelled.borrow() {
            return;
        }
        let job = tokio::select! {
            changed = cancelled.changed() => {
                let _ = changed;
                return;
            }
            job = jobs.recv() => match job {
                Some(job) => job,
                None => return,
            }
        };
        let session_key = (job.peer.clone(), FragmentId(job.fetch.fragment));
        if sessions.lock().await.get(&session_key).copied() != Some(job.session) {
            continue;
        }

        let response = send_reconcile_data_pages(
            &job.actor,
            &tx,
            job.fetch,
            job.snapshot_time_ms,
            admission_timeout,
            job.retention,
        );
        tokio::pin!(response);
        loop {
            tokio::select! {
                changed = cancelled.changed() => {
                    let _ = changed;
                    return;
                }
                changed = session_changes.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    if sessions.lock().await.get(&session_key).copied() != Some(job.session) {
                        // Dropping the response future cancels an in-progress
                        // actor RPC or outbound admission wait for the stale
                        // session without disturbing the replacement session.
                        continue 'jobs;
                    }
                }
                sent = &mut response => {
                    if !sent {
                        tx.cancel();
                        return;
                    }
                    continue 'jobs;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[test]
    fn reconcile_response_linear_fit_matches_prior_boundary_semantics() {
        let base = ReconcileResponse {
            fragment: u32::MAX,
            summaries: Vec::new(),
            session_id: u64::MAX,
            round_id: u64::MAX,
            page_seq: 128,
            next_after_key: String::new(),
            snapshot_revision: u64::MAX,
            snapshot_time_ms: u64::MAX,
            done: false,
            restart: true,
        };
        let empty = assert_reconcile_response_fit_matches_reference(base.clone(), "cursor", true);
        assert!(empty.summaries.is_empty());
        assert_eq!(empty.next_after_key, "cursor");
        assert!(empty.done);

        let summary = |key: String| KeySummary {
            content_digest: vec![0; 32],
            key,
            timestamp_ms: u64::MAX,
            logical_counter: 0,
            node_id: "node".to_string(),
            is_tombstone: true,
        };
        let singleton = assert_reconcile_response_fit_matches_reference(
            ReconcileResponse {
                summaries: vec![summary(format!("b{}", "x".repeat(600 * 1024)))],
                ..base.clone()
            },
            "a",
            true,
        );
        assert_eq!(singleton.summaries.len(), 1);
        assert!(
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileResponse(singleton.clone()))
            }
            .encoded_len()
                > ANTI_ENTROPY_PAGE_BYTES
        );
        assert!(singleton.done);

        let fixed_key = |prefix: char| format!("{prefix}{}", "x".repeat(300 * 1024));
        let truncated = assert_reconcile_response_fit_matches_reference(
            ReconcileResponse {
                summaries: vec![
                    summary(fixed_key('a')),
                    summary(fixed_key('b')),
                    summary(fixed_key('c')),
                ],
                ..base.clone()
            },
            "",
            true,
        );
        assert_eq!(truncated.summaries.len(), 2);
        assert!(!truncated.done);

        let long_middle = format!("b{}", "x".repeat(600 * 1024));
        let non_monotonic = ReconcileResponse {
            summaries: vec![
                summary("a".to_string()),
                summary(long_middle),
                summary("c".to_string()),
            ],
            ..base
        };
        let mut first_two = non_monotonic.clone();
        first_two.summaries.truncate(2);
        first_two.next_after_key = first_two.summaries[1].key.clone();
        assert!(
            Envelope {
                msg: Some(EnvelopeMsg::ReconcileResponse(first_two))
            }
            .encoded_len()
                > ANTI_ENTROPY_PAGE_BYTES
        );
        let fitted = assert_reconcile_response_fit_matches_reference(non_monotonic, "", true);
        assert_eq!(fitted.summaries.len(), 3);
        assert_eq!(fitted.next_after_key, "c");
        assert!(fitted.done);

        let mut oversized = ReconcileResponse {
            summaries: vec![summary("z".repeat(MAX_FRAME_BYTES / 2))],
            ..ReconcileResponse::default()
        };
        assert!(fit_reconcile_response_page(&mut oversized, "", true).is_err());
    }

    #[test]
    fn reconcile_data_reserves_terminal_flag_across_sequence_boundaries() {
        for page_seq in [0, 1, 127, 128, 16_383, 16_384, u64::MAX] {
            let mut page = ReconcileData {
                fragment: u32::MAX,
                entries: Vec::new(),
                session_id: u64::MAX,
                round_id: u64::MAX,
                chunk_seq: u64::MAX,
                page_seq,
                done: false,
            };
            let base_payload_len = reconcile_data_reserved_base_len(&page);
            let mut sizing = RepeatedFieldSizer::new(14, 2, base_payload_len);
            for value_len in [0, 1, 127, 128, 16_383, 16_384] {
                let entry = ReconcileEntry {
                    key: format!("key-{value_len}"),
                    value: vec![7; value_len],
                    timestamp_ms: u64::MAX,
                    logical_counter: 0,
                    node_id: "node".to_string(),
                    expires_at_ms: u64::MAX,
                    is_tombstone: true,
                };
                let item_len = entry.encoded_len();
                let predicted = sizing.encoded_len_with(item_len);
                page.entries.push(entry);
                let mut terminal = page.clone();
                terminal.done = true;
                assert_eq!(
                    predicted,
                    Envelope {
                        msg: Some(EnvelopeMsg::ReconcileData(terminal))
                    }
                    .encoded_len(),
                    "page_seq={page_seq}, value_len={value_len}"
                );
                assert_eq!(sizing.push(item_len), predicted);
                assert!(
                    Envelope {
                        msg: Some(EnvelopeMsg::ReconcileData(page.clone()))
                    }
                    .encoded_len()
                        <= predicted
                );
            }
        }
    }

    #[tokio::test]
    async fn session_replacement_cancels_in_progress_reconcile_response() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (actor, actor_handle) = BlockingPageActor::spawn(
            None,
            BlockingPageActor,
            BlockingPageState {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await
        .unwrap();
        let (peer, _frames, _responses) = test_peer_channel(1);
        let session_key = ("peer".to_string(), FragmentId(0));
        let original_session = InboundSession {
            session_id: 7,
            generation: 1,
        };
        let sessions: InboundSessions = Arc::new(Mutex::new(HashMap::from([(
            session_key.clone(),
            original_session,
        )])));
        let (session_changes, session_change_rx) = watch::channel(0u64);
        let (jobs, job_rx) = mpsc::channel(1);
        let responder = tokio::spawn(run_reconcile_responder(
            job_rx,
            peer,
            sessions.clone(),
            session_change_rx,
            Duration::from_secs(30),
        ));
        jobs.send(ReconcileResponseJob {
            retention: None,
            peer: "peer".to_string(),
            session: original_session,
            actor,
            fetch: ReconcileFetch {
                fragment: 0,
                keys: vec!["key".to_string()],
                session_id: original_session.session_id,
                round_id: 1,
                chunk_seq: 0,
            },
            snapshot_time_ms: 1,
        })
        .await
        .unwrap();
        started.notified().await;

        sessions.lock().await.insert(
            session_key,
            InboundSession {
                session_id: 8,
                generation: 2,
            },
        );
        session_changes.send_modify(|generation| *generation += 1);
        drop(jobs);
        tokio::time::timeout(Duration::from_secs(1), responder)
            .await
            .expect("stale responder remained blocked on its actor RPC")
            .unwrap();

        release.notify_one();
        actor_handle.abort();
        let _ = actor_handle.await;
    }

    fn reference_fit_reconcile_response_page(
        response: &mut ReconcileResponse,
        after_key: &str,
        source_done: bool,
    ) -> Result<usize, usize> {
        let original_len = response.summaries.len();
        loop {
            response.next_after_key = response
                .summaries
                .last()
                .map(|summary| summary.key.clone())
                .unwrap_or_else(|| after_key.to_string());
            response.done = source_done && response.summaries.len() == original_len;
            let encoded_len = message_field_len(12, response.encoded_len());
            if encoded_len > MAX_FRAME_BYTES && response.summaries.len() <= 1 {
                return Err(encoded_len);
            }
            if encoded_len <= ANTI_ENTROPY_PAGE_BYTES
                || (response.summaries.len() <= 1 && encoded_len <= MAX_FRAME_BYTES)
            {
                return Ok(encoded_len);
            }
            response.summaries.pop().expect("non-singleton page");
        }
    }

    fn assert_reconcile_response_fit_matches_reference(
        response: ReconcileResponse,
        after_key: &str,
        source_done: bool,
    ) -> ReconcileResponse {
        let mut expected = response.clone();
        let expected_len =
            reference_fit_reconcile_response_page(&mut expected, after_key, source_done);
        let mut actual = response;
        let actual_len = fit_reconcile_response_page(&mut actual, after_key, source_done);
        assert_eq!(actual_len, expected_len);
        assert_eq!(actual, expected);
        if let Ok(encoded_len) = actual_len {
            assert_eq!(
                encoded_len,
                Envelope {
                    msg: Some(EnvelopeMsg::ReconcileResponse(actual.clone()))
                }
                .encoded_len()
            );
        }
        actual
    }
}
