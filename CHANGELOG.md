# Release notes

## 0.10.0 (unreleased)

Corrective release for the P1/P2 findings tracked under #114. Peer protocol
6, application protocol 4 and existing storage/checkpoint/journal formats remain
unchanged. The breaking change is the SDK's configurable decoded-response
admission allowance, defaulting to 64 MiB (#153): responses accepted by 0.9.0
can now require an explicitly larger allowance. No release is published by these
pull requests, and current-format stores require no conversion.

- Persist membership-transition journal updates before committing in-memory
  state, and re-establish durability barriers on retries/reopen (#142).
- Bound named-table actor pools and pending-empty namespace metadata; reclaim
  only idle/unowned runtime state while preserving data, persistence ownership
  and query incarnations (#143).

## 0.9.0

This is a fresh-deployment release. Deploy the database, protocol, query and SDK
crates together at `0.9.0`; there is no rolling upgrade, old-store adoption or
backwards-compatible wire mode.

- Add cancellation-safe admission draining, configured/stable local persistence
  fences, queued response ownership and receive/apply shutdown barriers.
- Add owned startup, typed lifecycle/readiness/blockers, replication progress and
  bounded backend-neutral node/table/fragment/peer/pool/worker observations.
- Add generation allowlist changes that close old sessions and drain accepted
  work before publishing a new receiver policy.
- Add stable bounded checkpoint export/import, authenticated chunks, durable
  resumable cursors, replica floor evidence and guarded staged membership.
- Add strict acknowledgement policies and actual memory/store/stable evidence
  tied to frozen replica identities and operation/version floors.
- Use exact peer protocol 6 and application protocol 4. Remove capability and
  version-range negotiation, retired messages and unused wire fields.
- Use RocksDB record/manifest format 3, checkpoint format ACTCP003 and membership
  journal ACTMEM04. Non-current formats fail closed without adoption or repair.
- Remove Ed25519 process keys, frame signatures and their extra frame overhead.
  Authentication uses the transport's TLS policy and optional shared-secret
  challenge/response. Generation checks are operational controls, not proof of
  credential revocation.
- Remove compatibility reexports, unbounded replication entry points, no-op
  tombstone collection, always-zero counters and unimplemented status fields.
  Use the canonical actor/protocol APIs and bounded sender constructor.
- Keep one typed node lifecycle, separate drain ownership, and an always-present
  shutdown stage. Retain explicit omissions only for genuinely unavailable data.
- Update all documentation and examples to the current APIs and fresh-deployment
  contract.

See [deployment](docs/support-api-migration.md), [draining](docs/draining.md),
[observations](docs/observability.md), [peer authority](docs/peer-authority.md),
[checkpoints](docs/stable-checkpoints.md), [membership](docs/staged-membership.md)
and [write evidence](docs/write-concerns.md).

Previous release notes are available in Git history.
