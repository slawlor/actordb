# Test strategy

Three layers, each catching what the others can't.

## Unit tests (`cargo test --lib`)

Fast, in-process tests of the building blocks: `FragmentState` apply/LWW
merge semantics, `VersionedValue::wins_over` ordering (including the
tombstone-wins-ties rule), `HlcClock` tick/observe monotonicity, worker-pool
admission and shed counting, token-bucket behavior, seahash fragment mapping,
and store encode/decode round-trips. These pin the semantics that the
integration tests rely on.

Bounded replication-ingress unit tests additionally pin that a full mutation
lane publishes a persistent reset generation, that the independent control
lane fails fast at its cap, and that peer/reconcile signals remain available.
`retiring_reconcile_consumer_does_not_close_shared_connection` forces both
reconcile reply variants to race sender retirement after control-lane admission
and proves the multiplexed peer connection remains usable.
The placement lease test proves revocation waits for an already-admitted
mutation before changing the serving decision.

## Integration tests (`actordb/tests/cluster.rs`)

The cluster suite runs over real TCP on localhost with multiple `Node`s in one
process — the bulk of behavioral coverage:

- **Basics**: `single_node_put_get`, `two_nodes_replicate`,
  `three_node_mesh_converges`, `lww_last_write_wins_everywhere`.
- **Routing**: `fragments_route_and_isolate`,
  `disjoint_fragments_route_to_owner`, `replica_fanout_reaches_other_holder`.
- **Failures**: `dropped_connection_redials`,
  `restarted_node_writes_are_accepted`, `late_joiner_receives_full_state`,
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
  `fetched_reconcile_value_retries_failed_local_store_fence`.
- **Persistence**: `persistence_roundtrip`,
  `persistence_keeps_gossiped_winner`, `persistence_flushes_on_shutdown`,
  `memory_store_exercises_persistence_plumbing`.
- **Multi-key**: `multi_key_fanout`, `multi_key_partial_failure`,
  `batch_put_fails_over_after_a_stale_owner_returns_unavailable`,
  `mutation_relay_scans_past_the_origin_to_a_connected_owner`.
- **Backpressure**: `background_saturation_leaves_high_priority_p99_unaffected`,
  `queue_cap_shed_is_explicit_and_counted`,
  `forwarded_priority_preserved_end_to_end`,
  `forwarded_single_put_returns_remote_backpressure`.
- **Remote mutation failures**:
  `forwarded_put_returns_remote_storage_failure`; node unit tests reject
  unspecified statuses and malformed result order/cardinality.
- **Placement**: `placement_migration_no_lost_writes` (single writer during a
  live migration asserts zero lost writes),
  `placement_rolling_restart_delta_reconcile_only` (restarted node converges
  with zero full-state sync entries),
  `placement_bootstrap_fetch_requires_verification_round`,
  `placement_drain_waits_for_buffer_reset`, and
  `placement_readd_while_draining_cancels_teardown`.

Shared helpers (`wait_for` poll-until-timeout) live in
`actordb/tests/common/mod.rs`.

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

CI additionally enforces `cargo fmt --check`, Clippy with warnings denied,
rustdoc with warnings denied, and benches.
