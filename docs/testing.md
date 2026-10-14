# Test strategy

Three layers, each catching what the others can't.

## Unit tests (`cargo test --lib`)

Fast, in-process tests of the building blocks: `FragmentState` apply/LWW
merge semantics, `VersionedValue::wins_over` ordering (including the
tombstone-wins-ties rule), `HlcClock` tick/observe monotonicity, worker-pool
admission and shed counting, token-bucket behavior, seahash fragment mapping,
and store encode/decode round-trips. These pin the semantics that the
integration tests rely on.

## Integration tests (`actordb/tests/cluster.rs`)

More than 40 tests over real TCP on localhost with multiple `Node`s in one
process — the bulk of behavioral coverage:

- **Basics**: `single_node_put_get`, `two_nodes_replicate`,
  `three_node_mesh_converges`, `lww_last_write_wins_everywhere`.
- **Routing**: `fragments_route_and_isolate`,
  `disjoint_fragments_route_to_owner`, `replica_fanout_reaches_other_holder`.
- **Failures**: `dropped_connection_redials`,
  `restarted_node_writes_are_accepted`, `late_joiner_receives_full_state`,
  `worker_kill_triggers_supervised_restart`.
- **TTL/tombstones**: `ttl_expiry_removes_key`,
  `ttl_expiry_is_persisted_as_a_versioned_tombstone`,
  `ttl_expiry_does_not_resurrect_an_older_partitioned_value`,
  `startup_rewrites_expired_durable_values_as_tombstones`,
  `delete_propagates_to_replicas`, `delete_survives_restart_and_rejoin`.
- **HLC**: `hlc_orders_writes_under_clock_skew` (simulated skew via the
  hidden `clock_skew_ms` knob).
- **Replication internals**: `sender_buffer_reset_and_reconcile`,
  `stale_session_batches_discarded`.
- **Persistence**: `persistence_roundtrip`,
  `persistence_keeps_gossiped_winner`, `persistence_flushes_on_shutdown`,
  `memory_store_exercises_persistence_plumbing`.
- **Multi-key**: `multi_key_fanout`, `multi_key_partial_failure`.
- **Backpressure**: `background_saturation_leaves_high_priority_p99_unaffected`,
  `queue_cap_shed_is_explicit_and_counted`,
  `forwarded_priority_preserved_end_to_end`.
- **Placement**: `placement_migration_no_lost_writes` (single writer during a
  live migration asserts zero lost writes),
  `placement_rolling_restart_delta_reconcile_only` (restarted node converges
  with zero full-state sync entries).

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
cargo xtask coverage             # coverage report (CI enforces it)
# Docker scenarios run in CI; see docker/run-scenario.sh to run locally.
```

CI additionally enforces `cargo fmt --check`, Clippy with warnings denied,
rustdoc with warnings denied, and benches.
