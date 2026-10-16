# Test strategy

Three layers, each catching what the others can't.

## CI feature coverage and caches

CI tests the default workspace once, including the offered RocksDB backend,
then tests without default features using
`cargo test --workspace --exclude docker-tests --no-default-features --locked`.
The exclusion matters: `docker-tests` enables `actordb`'s default features
through its dependency, even when the workspace command disables defaults.
The default workspace run still covers that integration crate. Clippy checks
all targets in both configurations, alongside the existing Rust 1.88,
formatting, release-documentation, and benchmark-compilation gates.

Compile jobs restore dependency artifacts with `Swatinem/rust-cache`. Stable
debug tests and Clippy share a cache; Rust 1.88 and the release docs/benchmark
jobs use separate caches. The action also keys artifacts by the actual Rust
compiler, Cargo manifests/lockfile, and compiler environment, and does not cache
workspace build outputs by default. Coverage keeps its separate instrumented
cache. Docker already uses cargo-chef layers and the GitHub Actions Buildx
cache backend. A cold cache still performs every build and check.

## Unit tests (`cargo test --lib`)

Fast, in-process tests of the building blocks: `FragmentState` apply/LWW
merge semantics, `VersionedValue::wins_over` ordering (including the
tombstone-wins-ties rule), `HlcClock` tick/observe monotonicity, worker-pool
admission and shed counting, token-bucket behavior, seahash fragment mapping,
and store encode/decode round-trips. These pin the semantics that the
integration tests rely on.

Storage-contract tests additionally cover fail-closed manifest versions and
metadata schemas, exact identity and exclusive attachment, concurrent bind,
strict non-mutating rejection of legacy databases, explicit adoption with
UTF-8/fragment-hash/wire-budget validation, bounded prefix reads, direct-write
validation, and synchronous isolated/idempotent range deletion. Node lifecycle
tests abort startup inside a gated store load and prove the lease remains
fail-closed, while a fully joined failed startup releases it for retry.
Stable-fence tests distinguish ordinary buffered commits from the explicit
backend fence and verify that RocksDB can force its WAL under `AsyncWal`.

Protocol-v3 pagination tests cover lexicographically ordered/exclusive summary
cursors (including the empty-string key), revision-drift detection, zero-limit
clamping, metadata-byte bounds, pinned TTL tombstone synthesis shared by
summaries and exact-value reads, bounded `GetVersionedPage` cloning, and
summary-page metadata. Wire helpers separately
test the exact encoded-size boundary, the 1 MiB soft-page/16 MiB hard-frame
distinction, rejection before oversized encoding/allocation, repeated-field
decode-amplification preflight, and parity with direct prost encoding.
Replication tests also pin exact-byte streaming batch splits, peer-writer
byte-budget backpressure, shared physical-connection reconciliation credits,
FIFO handoff between competing fragments, nonblocking reader responses, their
bounded-burst priority lane and byte reserve, and stale-session responder
cancellation. A 4097-item remote batch regression crosses the allocation-safe
wire cardinality boundary without dropping the connection. A
bidirectional ten-fragment cluster test combines symmetric near-limit values,
the minimum writer byte budget, and simultaneous reconciliation.

Bounded replication-ingress unit tests additionally pin that a full mutation
lane publishes a persistent reset generation, that the independent control
lane fails fast at its cap, and that peer/reconcile signals remain available.
`retiring_reconcile_consumer_does_not_close_shared_connection` forces both
reconcile reply variants to race sender retirement after control-lane admission
and proves the multiplexed peer connection remains usable.
Sender-registry tests additionally hold an individual report lock while proving
the node-global sender/report registries and placement publication remain
available, reject a same-key replacement by exact task/report identity, and
prove the per-fragment finalization seal blocks recreation only until its
cancellable handoff attempt ends.
The placement lease test proves revocation waits for an already-admitted
mutation before changing the serving decision.
`dispatcher_restart_is_republished_to_the_fragment_router` exercises the full
supervisor-to-router replacement path, while
`stale_dispatcher_monitor_cannot_overwrite_a_readded_fragment` pins its
generation/actor-identity publication fence.

## Integration tests (`actordb/tests/cluster.rs`)

The cluster suite runs over real TCP on localhost with multiple `Node`s in one
process — the bulk of behavioral coverage:

- **Basics**: `single_node_put_get`, `two_nodes_replicate`,
  `three_node_mesh_converges`, `lww_last_write_wins_everywhere`.
- **Routing**: `fragments_route_and_isolate`,
  `disjoint_fragments_route_to_owner`, `replica_fanout_reaches_other_holder`.
- **Failures**: `dropped_connection_redials`,
  `restarted_node_writes_are_accepted`,
  `late_joiner_converges_via_immediate_reconciliation`,
  `failed_immediate_reconciliation_is_not_counted_and_retries`,
  `reconciliation_paginates_summaries_and_fetches_without_restart_storms`,
  `reconcile_cursor_continues_after_revision_drift_for_verified_multi_pass`,
  `worker_kill_triggers_supervised_restart`, and
  `incompatible_hello_capabilities_are_rejected`.
- **TTL/tombstones**: `ttl_expiry_removes_key`,
  `ttl_expiry_is_persisted_as_a_versioned_tombstone`,
  `ttl_expiry_does_not_resurrect_an_older_partitioned_value`,
  `startup_rewrites_expired_durable_values_as_tombstones`,
  `delete_propagates_to_replicas`, `delete_survives_restart_and_rejoin`.
- **HLC**: `hlc_orders_writes_under_clock_skew` (simulated skew via the
  hidden `clock_skew_ms` knob).
- **Replication internals**: `sender_buffer_reset_and_reconcile`,
  `live_connected_overflow_reconciles_without_reconnect`,
  `stale_session_batches_discarded`,
  `session_started_before_actor_installation_survives_placement_race`,
  `stale_reconcile_round_cannot_complete_live_reset`,
  `malformed_reconcile_data_is_rejected_and_retried`,
  `same_session_replay_does_not_clear_reconcile_failure`,
  `silent_reconcile_phase_times_out_and_retries`,
  `inbound_mutation_after_snapshot_invalidates_reset_completion`,
  `memory_applied_ack_does_not_force_a_receiver_store_flush`,
  `replication_ack_waits_for_the_receiver_store_fence`,
  `replication_rejects_an_ack_below_the_configured_durability`,
  `replication_nack_retries_the_exact_batch_after_store_failure`,
  `failed_reconcile_repair_cannot_be_hidden_by_barrier_ack`,
  `fetched_reconcile_value_retries_failed_local_store_fence`, and
  `reconcile_barrier_rejects_a_replaced_fragment_incarnation`.
  The gated receiver tests disable incidental apply/snapshot flushes when
  testing ACK durability. Every `GatedStore` test retains a release-on-drop
  guard so assertion failure or cancellation unblocks synchronous store calls
  before runtime teardown; `gated_store_release_guard_unblocks_a_write_on_unwind`
  verifies that a blocked write completes after an injected panic.
- **Replication-drained shutdown**:
  `replication_drained_shutdown_fences_an_immediate_write`,
  `drain_requires_no_change_verification_round`,
  `drain_ignores_stale_old_session_ack_after_rotation`,
  `drain_disconnect_reconnect_retries_same_generation`,
  `manual_placement_drain_freezes_target_and_keeps_replication_runtime_alive`,
  `replication_drain_timeout_retries_the_fenced_overflow_snapshot`,
  `concurrent_replication_drain_callers_keep_independent_deadlines`,
  `replication_drain_honors_store_committed_receiver_fence`,
  `store_committed_drain_rejects_a_storeless_receiver_ack`,
  `store_committed_drain_persists_to_the_offered_rocksdb_backend`, and
  `replication_report_survives_a_later_local_flush_failure`.
- **Persistence**: `persistence_roundtrip`,
  `persistence_keeps_gossiped_winner`, `persistence_flushes_on_shutdown`,
  `rocksdb_manifest_rejects_wrong_identity_before_network_bind`,
  `rocksdb_store_allows_only_one_live_node_attachment`,
  `shutdown_flush_failure_is_retryable_across_all_fragments`,
  `cancelled_shutdown_waiter_resumes_retained_final_flush`,
  `shutdown_rejects_new_calls_and_waits_for_an_admitted_put`,
  `shutdown_materializes_expiry_before_the_final_flush`, and
  `memory_store_exercises_persistence_plumbing`. The supervision unit test
  `quiesce_drains_preaccepted_worker_messages_before_flushing` proves accepted
  worker work reaches the authoritative final flush, while
  `standalone_dispatcher_drain_flushes_buffered_writes` pins the public actor
  contract. `failed_supervisor_start_joins_partial_worker_tree` and
  `failed_snapshotter_start_joins_completed_supervisor_children` cover nested
  startup rollback; `cancelled_subtree_start_remains_registered_until_startup_completes`
  covers the placement-task cancellation race.
- **Routing and batching**: `multi_key_fanout`, `multi_key_partial_failure`,
  `batch_operations_fail_over_after_a_stale_owner_returns_unavailable`,
  `timed_out_single_put_retries_the_exact_stamped_operation`,
  `timed_out_single_get_tries_the_next_holder`,
  `mutation_relay_scans_past_the_origin_to_a_connected_owner`,
  `remote_batches_split_at_wire_item_limit_without_dropping_connection`,
  `remote_batches_split_at_encoded_byte_limits_without_false_misses`.
  Receive-admission unit tests hold simultaneous incomplete maximum frames on
  accepted and outbound-dialed handlers, reject an additional body before
  reading it, then verify unrelated peer traffic and replacement handshakes
  remain usable after capacity returns. Separate tests cover completed-frame
  retention, empty/oversized bodies, EOF, body timeout, cancellation, task abort,
  and configuration validation. Maintenance interval regressions reject zero
  and unrepresentable cleaner, snapshotter, and enabled placement timers before
  transport bind, while preserving ignored placement settings in static mode.
  Node and wire unit tests separately verify exact Prost length accounting,
  positional ordered-prefix validation with duplicate keys, the shed-bit hard
  boundary, actor-side `BatchGet` result sizing, UTF-8-safe diagnostic
  truncation, valid provenance bounds, and the reserved full-frame response
  lane under complete producer saturation. Actor tests prove a queue-cap-one
  continuation releases before admitting its next key and a byte-page suffix
  consumes no rate-limit tokens. They also cover orderly drain returning an
  admitted prefix and cancellation retaining capacity until worker completion
  without admitting the suffix. A raw-peer test blocks the batch actor through
  its deadline, verifies an empty unavailable response, and then exchanges
  another request on the same connection while that actor remains blocked.
  Direct worker coverage checks exact Prost response boundaries, duplicate
  positions, misses, suffix resumption, and empty input. A production TCP test
  verifies per-key background shedding and a later high-priority batch over the
  same connection. The
  connection tests also prove that a reconcile request/fetch racing a
  non-serving fragment cannot tear down unrelated multiplexed traffic, actor
  unavailability cannot become a false single-read miss, and direct or relayed
  single-read responses are pinned to the expected authenticated peer.
- **Relay admission/lifetime**: `silent_owner_ingress_is_bounded_by_shared_relay_count_and_key_bytes`
  exercises the production peer reader with a silent owner: both aggregate
  count and key-storage saturation reject immediately, return typed mutation
  unavailability, never fabricate read misses, and preserve the same connection
  for a later frame. Key-accounting tests charge empty keys and unused String/Vec
  capacity and verify rollback when the byte budget fails. Deterministic tests
  cancel registration before timer acknowledgment and cancel blocked outbound
  admission, checking registry, operation-fence, and timer-permit reclamation.
  Exact owner/origin replacement and reused relay ids cannot clear newer work;
  a raw-peer test ignores both read and mutation responses from the same peer
  id on the wrong physical channel. Timeout and completed-response tests verify
  unavailable semantics and cancellation of the owned timer.
  A successful raw-peer batch relay preserves duplicate positions, all operation
  stamps, per-item hop increments, and distinct downstream statuses; terminal
  responses reclaim both relay capacity and the operation fence. Origin-loss
  tests also verify timer-only cleanup when the connection tail cannot run.
- **Backpressure**: `background_saturation_leaves_high_priority_p99_unaffected`,
  `queue_cap_shed_is_explicit_and_counted`,
  `forwarded_priority_preserved_end_to_end`,
  `forwarded_single_put_returns_remote_backpressure`. Actor unit tests add
  deterministic coverage for standalone rate/cap accounting, immediate worker
  send rollback, cancellation retaining its slot until worker completion,
  reclaiming only a dead worker generation's active and queued reservations,
  clean drain when worker replacement races shutdown, and proving a blocked
  background `Get` or actor-routed batch read does not stall high-priority
  reads or health queries.
  Node read-service tests additionally prove nonwaiting queue/byte shedding,
  independent priority admission/execution, expired queued jobs skipping actors,
  stale route publication rejection, sticky physical cancellation without watch
  subscribers, and shutdown dropping queued leases while actor-held keys remain
  charged. The real sticky-worker lease regression covers cancellation and
  worker death without suffix admission. Count-exhaustion coverage combines a
  cancelled actor-held request with all execution workers occupied and a spare
  queue slot/byte budget. Additional regressions verify preexpired and unavailable
  single-read silence, closed-runtime registration, and response-lane failure
  closing only the originating physical channel while releasing read admission.
  A production connection-handler test
  holds both single and batch reads while an unrelated pending response completes
  on the same physical socket, before actor release or the read deadline.
- **Remote mutation failures**:
  `forwarded_put_returns_remote_storage_failure`; node unit tests reject
  unspecified statuses and malformed result order/cardinality.
  `mixed_batch_put_retries_only_unavailable_positions_with_exact_stamps`
  exercises three holders, shrinking noncontiguous retry subsets, duplicate
  keys, unchanged stamps, and terminal outcomes.
  `mixed_batch_put_malformed_subset_exhaustion_preserves_terminal_results`
  rejects an entire malformed subset response and returns network errors only
  for positions still unresolved after holder exhaustion.
- **Placement**: `placement_migration_no_lost_writes` (single writer during a
  live migration asserts zero lost writes),
  `placement_rolling_restart_delta_reconcile_only` (restarted node converges
  through reconciliation with the retired sync counter still zero),
  `placement_bootstrap_fetch_requires_verification_round`,
  `placement_drain_waits_for_buffer_reset`, and
  `placement_readd_while_draining_cancels_teardown`,
  `placement_readd_waits_for_failed_final_flush_and_reloads`,
  `placement_retires_rocksdb_prefix_after_stable_handoff`, and
  `placement_prefix_delete_failure_retries_before_stopped`. Replication unit
  coverage also proves a `StoreCommitted` ACK cannot retire a privately
  strengthened `StableCommitted` drain barrier.

Shared helpers (`wait_for` poll-until-timeout) live in
`actordb/tests/common/mod.rs`.

Manual Criterion coverage in `actordb/benches/sender_registry.rs` compares one
and 64 fully replicated fragments at write concurrency 1/32/64 with 100-byte
and 1-MiB values. It times source apply plus fan-out while awaiting replica
visibility outside the measurement, exposing value-size, registry-cardinality,
and cross-fragment contention regressions without turning sender overflow
recovery into the measured workload. Fixed key sets keep retained cache memory
bounded across Criterion's calibrated iterations.

`actordb/benches/replication.rs` also has a low-timer `buffered_catchup`
group. Each measured iteration preloads 64/256/1024/4096 compact records into a
disconnected sender, then measures connect through its observed ACK on fresh
storeless nodes with a 1 ms sender tick. Preload, verification, and shutdown
are outside the reported duration. This exercises production batch
construction without the default 100 ms floor and catches a return of
quadratic whole-message protobuf sizing.

## Docker multi-node scenarios (`docker-tests/`)

The `docker-tests` crate builds real container images and runs scripted
multi-node scenarios in CI (`docker-compose.yml`, `docker/`):

- **Replication**: three nodes form a mesh, writes on one converge everywhere.
- **SIGKILL + restart reconvergence**: a node is SIGKILLed mid-write and
  restarted; it must reconverge (new session, redial, reconcile) while readers
  on the surviving nodes keep working.

These catch what localhost integration tests can't: real process death, DNS
and container-network behavior, and the full restart path including session
re-establishment.

## Running them

```bash
cargo test -p actordb            # unit + integration
cargo run --bin xtask -- coverage # coverage report (CI enforces it)
# Docker scenarios run in CI; see docker/run-scenario.sh to run locally.
```

CI additionally pins Rust 1.88 as the workspace MSRV and enforces
`cargo fmt --check`, Clippy with warnings denied, rustdoc with warnings denied,
and benches.
