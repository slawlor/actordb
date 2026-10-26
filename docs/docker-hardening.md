# Docker cluster failure coverage

The hardening matrix runs four independent database processes, real RocksDB
volumes, and a separate SDK-only checker. Process death, durable reopen,
DNS/redial and network loss complement the Rust in-process cluster tests.

Every applicable PR runs the full matrix after the existing replication,
restart and external-client scenarios. The Docker budget is **90 minutes**
including image builds. Checker convergence is bounded at 180 seconds and
individual processes at 240 seconds; shutdown has its own deadline.
The matrix also has a 45-minute overall deadline and handles termination signals
by attempting diagnostics and scoped cleanup. A hard-killed runner cannot
guarantee cleanup or artifact upload.

| Boundary | Required evidence |
| --- | --- |
| Genesis and one → two → three owners | Actual Serving authority and exact records on each owner |
| Routing ingress without peers | Errors, never false misses, empty COUNT or partial success |
| Replica partition | Local stable AP writes, failed isolated majority, exact live/delete/TTL LWW oracle |
| Sender history overflow | Deliberately small queues reset and repair from authoritative data |
| Repeated rolling SIGKILL | Stable surviving-majority writes, new boot identity and exact catch-up |
| Backend failures/timeouts | No false stable receipt; unknown writes are not transparently retried |
| Fourth owner crashes during bootstrap | Gated acquisition observed; old owners retained; restart catches up |
| Shrink with a missing target | Old owner stays Draining and retains its prefix until stable handoff |
| Retired owner restarts | No stale authority or durable-prefix resurrection |
| Full hard outage | Same volumes, fresh explicit epoch, first candidate cannot serve alone |
| SIGTERM | Zero exit only after request/apply/storage ownership and stable fences finish |

Default data covers six fragments, three tables, Unicode/NUL values, a literal
internal-looking key and large opaque values. 128 seed rows per table/fragment
force paginated reconciliation. The saved oracle contains exact values,
physical/logical versions, writer IDs, expiry and tombstone bits; it is never
rebuilt from recovered replicas. Stable receipts and fragment barriers define
the floors expected to survive. Unflushed acknowledged tails are not claimed
to survive complete failure.

After killing the new bootstrap candidate, the driver separately observes
Serving authority and completed surviving sender lanes before submitting a
strict-majority write. Exact cached records alone do not prove delivery is
ready. Mutation timeouts still fail the scenario and are never retried;
failures retain full receipt diagnostics and live node/sender snapshots.
The legacy restart scenario also gates the kill on survivor readiness and
the restart on observed peer death, so a fast reboot cannot hide the outage.

## Control and failure scope

The cluster-node control socket is PRIVATE TEST-ONLY, unauthenticated, and
must never be deployed publicly. Compose publishes no host ports. Typed control
requests have size/deadline limits; validated placement intent is persisted
atomically. Next-boot-only intent configures an absent target before killing it.

Peers use peer-net; SDK/control uses control-net. Docker actually disconnects
the peer bridge while retaining access for assertions. Process mode substitutes
a test-only peer transport gate; it is not proof that Docker networking passed.
Docker bridge detachment is inspected directly, then routed availability and
isolated-quorum outcomes prove the outage. Idle TCP peers can stay registered
after packet loss, so peer count alone is not an outage signal.
Injected store failure/delay tests backend contracts, not kernel ENOSPC,
filesystem corruption or power loss. Growth/shrink uses real dynamic placement
and durable handoff, not the separate staged-membership journal protocol.

Generated Compose projects own only their test containers/networks/volumes.
Cleanup is attempted on success, failure and handled cancellation. Logs, status
snapshots and exit state are captured before
cleanup and uploaded by CI for 14 days. Existing artifact directories are not
overwritten.

## Running

After building the existing server/client images, run:

    python3 docker/cluster-hardening.py --cycles 3 --records-per-fragment 128

The image build defaults to four Cargo jobs to bound native compilation memory.
For smaller Docker Desktop VMs, build each target with a lower limit:

    docker build --build-arg CARGO_BUILD_JOBS=2 --target runtime \
      -f docker-tests/Dockerfile . -t actordb-docker-test:latest
    docker build --build-arg CARGO_BUILD_JOBS=2 --target client-runtime \
      -f docker-tests/Dockerfile . -t actordb-docker-client-test:latest

Without Docker:

    cargo build --package docker-tests --package docker-client-tests
    python3 docker/cluster-hardening.py --backend process \
      --node-bin target/debug/actordb-docker-test \
      --client-bin target/debug/actordb-docker-cluster-client \
      --artifacts test-artifacts/local-hardening --cycles 1

Process mode still has separate PIDs, TCP, SIGKILL/SIGTERM and RocksDB reopens.
CI Docker remains required. Up to five rolling cycles and 256 seed rows per
fragment/table support additional stress with the same bounded waits.

Missing-target retirement is held for 35 seconds with every fragment prefix
checked; the first full-outage candidate is held alone for 40 seconds with
availability errors asserted. These deliberate fault windows exceed one full
reconciliation/bootstrap timeout rather than checking a momentary startup state.

