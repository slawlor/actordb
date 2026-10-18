# External ActorDB SDK scenario

This executable runs in a separate process or fourth Docker container. Its
normal dependencies are the Rust SDK, Tokio, Clap, and SeaHash. It never starts
or links a database node, actor runtime, storage backend, or protobuf compiler.

```
actordb-docker-client-test \
  --endpoint node-a:7449 --endpoint node-b:7449 --endpoint node-c:7449 \
  --credential 0123456789abcdef0123456789abcdef --timeout-secs 180
```

Endpoints accept DNS names and must identify node-a, node-b, and node-c in that
order. Nodes use six fragments: 0/1 on A/B, 2/3 on B/C, and 4/5 on C/A.
The credential shown is a public fixture for isolated tests.

The executable verifies all three replicas before printing
`CLIENT_HEALTHY_OK` and `CLIENT_WAITING_FOR_NODE_A_FAILURE`. The driver then
kills A. The client observes socket failure and peer counts dropping to one,
explicitly connects to B, verifies complete data/aggregates, and writes a new
fragment-4 key through B to C. At `CLIENT_FAILOVER_OK`, the driver restarts A.
The client verifies A's serving fragment-4 copy contains the new key and
complete baseline data, then prints `CLIENT_RECOVERY_OK` and `SCENARIO_OK`.

Every transition requires observed state; polling does not substitute elapsed
time for a successful assertion. The entire scenario has a configurable deadline
of at most 180 seconds. Mutations are issued once; reads and queries are polled
for eventual convergence. The driver owns node lifetimes and cleanup.
