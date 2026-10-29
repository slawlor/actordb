# Release notes

## 0.11.1 (unreleased)

- Index replication senders by fragment so mutation fanout visits only that
  fragment's replicas (#190). Preserve exact sender identity, retirement and
  admission fences under the existing registry mutex, and remove empty buckets.
  Add repeated lookup and Node write throughput measurements, including
  steady-state comparisons and controls for replication timer feedback; see
  [benchmark results](actordb/benches/sender_registry_results.md).
- Use `std::sync::RwLock` for fragment/table caches; cache guards protect
  synchronous work only. Keep async persistence/publication locks where
  guards span acquisition or I/O, and migrate contention/snapshot fixtures
  to preserve cancellation, ordering and revision checks without runtime
  blocking or a std guard across `.await`.
- Shorten fragment write critical sections by transferring owned worker values
  into the cache, preparing persistence copies and summary metadata before
  acquiring mutation locks, and releasing replaced payloads after unlocking.
- Compare buffered-write winners by their LWW version without a redundant
  full-payload equality check, preserving conflict checks, TTL selection,
  ordered persistence admission and revision publication.
- Add a mixed-priority fragment write benchmark and recorded before/after
  results. Local 24-writer, 1 MB throughput improved about 56–60%; smaller and
  single-writer workloads were mixed. See
  [benchmark results](actordb/benches/fragment_writes_results.md).

## 0.11.0

A hard application RPC migration plus the P1/P2 corrective changes tracked
under #114. All four libraries advance together. The peer protocol (6) and
current storage/checkpoint/journal formats are unchanged; application clients
use the new unary `actordb.rpc.v1.ActorDb/Call` contract, not the old TCP protocol.

- Replace the serialized application socket with typed, object-safe RPC client
  and service traits. gRPC is default-enabled in SDK and server, and removable
  with `default-features = false` for alternate frameworks.
- Remove the application hello/auth handshake, connection identity, credentials,
  frame envelope and request IDs. Authentication, TLS and ACL hooks belong to
  the selected RPC adapter, not the Node service or generic SDK.
- Preserve strict write-concern evidence, table/query/scan semantics, bounded
  decoding and cancellation-safe ownership. Concurrent RPCs use aggregate
  materialization reservations; lost mutation replies are never replayed.
- Migrate examples and all Docker scenarios to real gRPC. Keep routing outages,
  partitions, crashes, growth/shrink and explicit full-outage recovery mandatory.
- Group application bulk reads into bounded actor/peer pages, retaining shared
  node memory admission through cancellation and reply encoding. Batch independent
  writes in ordered waves with per-item TTLs and exact-stamp failover, preserving
  duplicate positions and accepted mutation ownership (#188).
- Size each bulk reply result once with exact incremental protobuf accounting,
  removing quadratic response walks without relaxing message limits (#189).
- Bound incomplete gRPC request bodies by a server-owned receive deadline,
  releasing materialization reservations without cancelling an already
  accepted mutation or its independent concern wait (#185).
- Preserve per-position `UnknownOutcome` across lost bulk-write replies and
  later holder rejections; unsent writes retain definite admission errors (#186).
- Flush borrowed winning batches through combined table/fragment namespace
  prefixes, avoiding uncharged intermediate full-value copies. Existing custom
  stores keep their owning batch entrypoint through a default bridge (#187).

- Persist membership-transition journal updates before committing in-memory
  state, and re-establish durability barriers on retries/reopen (#142).
- Bound named-table actor pools and pending-empty namespace metadata; reclaim
  only idle/unowned runtime state while preserving data, persistence ownership
  and query incarnations (#143).
- Accept every supported metadata SQL projection in the Rust SDK without
  retiring a valid application connection (#144).
- Route named-table scans through their actual table, treat public Node keys
  as literals, and preserve advertised user query limits independently of
  bounded namespace/canonical serialization overhead (#145, #146, #147).
- Page automatic expiry across all named tables within one aggregate actor
  work allowance and pinned expiry time; retain authoritative manual cleanup
  and drain opportunities between pages (#148).
- Keep invalid recovery placement in a bounded, cancellation-safe blocked
  wait, and isolate durable prefix retirement from unrelated placement
  publication using per-fragment backend-owned seals (#149, #154).
- Move readonly reconciliation summaries out of the multiplexed peer reader;
  reserve separate bounded fetch capacity and fence session/route/authority
  changes without blocking unrelated Get/ACK traffic (#150).
- Maintain incremental query-buffer ownership accounting and check increasing
  next-head reservations before cloning, preserving hard budgets and identity
  fences without repeated whole-buffer rescans (#151).
- Pace admitted local bulk writes fairly to configured completion capacity
  while retaining nonwaiting external admission and cancelled-job ownership
  through actual apply/fanout completion (#152).
- Admit decoded application responses before materialization, including
  collection growth, nested inspection/receipt data and portable conversion;
  retain bounded decode/conversion ownership across client clones (#153).
- Page reconciliation through ordered cached metadata without cloning an
  entire fragment, preserve the pinned TTL view, and atomically pair diagnostic
  snapshots with their corresponding mutation revision (#172, #167, #168).
- Share persistence record and byte admission across every table in a
  fragment while preserving independent table workers and retry ownership
  (#170); ensure standalone managed-actor persistence fences and orderly drain
  include their table children (#178).
- Validate durable table namespaces against the exact logical replicated
  record, so accepted near-limit keys and values remain persistable and
  transferable through checkpoints (#166).
- Keep managed workers alive after handled backend errors and caught backend
  panics, preserving unrelated admitted mailbox work and failed persistence
  winners for retry (#171).
- Reject unsolicited peer scan responses before allocating their result
  collections, and remove the unused outgoing-scan registry while preserving
  current inbound scan serving and healthy duplex sessions (#169).
- Fence gains and durable handoffs with fragment-scoped placement generations;
  unrelated topology changes no longer restart stable fragment catch-up, while
  same-fragment ABA changes remain fenced (#173).

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
