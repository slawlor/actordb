# Literal keys and table execution

Every public Node point/batch mutation and read treats its key as literal text
in KV. Reserved-prefix strings, NUL/control characters and Unicode are not
internal addresses. Table handles select their namespace explicitly and use
private already-qualified entrypoints; no public input gains a different table
identity merely by resembling the storage encoding.

Scans carry the selected table through planning, holder selection and actor
execution. Keys are decoded exactly once by the public Node/Table view. Limits
and offsets retain their per-fragment scan semantics. SQL ordering, LIMIT and
OFFSET remain global over the selected fragments.

User SQL and structured plans retain the public 32 key/metadata predicate,
16 KiB literal and 64 KiB raw SQL limits. Bound/structured plan serialization
has a separate computed allowance for all 32 literals plus quoting/syntax,
so small placeholder SQL can bind 32 full-size parameters. Fragment scope is independently
bounded and canonicalizes to one additional WHERE term. Internal physical plans
may include one namespace predicate and bounded hex/prefix expansion. They are
validated both under those execution allowances and after decoding back to the
logical user limits. Application clients never receive larger user allowances.

Physical peer SQL/key and stored-record formats are unchanged. Ordinary peer
queries retain their meaning. Older holders may explicitly reject newly
supported boundary queries; they cannot silently execute a lowered constraint.
All holders should use the corrective implementation for boundary support.

`shard::fragment_of_literal` hashes literal public Node keys. Existing
`shard::fragment_of` recognizes physical table/storage namespace encoding and
retains its prior mapping contract for storage and protocol embeddings.

The diagnostic `Node::get_all` still includes every live table this node holds.
Its display decodes KV keys but retains other tables' physical keys. When those
representations collide, it returns an explicit error rather than overwrite
one record in its HashMap. Use per-table queries for unambiguous extraction.
