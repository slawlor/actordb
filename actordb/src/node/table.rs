//! Logical table views over one node.
//!
//! A table view keeps the existing node, peer listener, and replication
//! channels. Its keys are encoded into a reserved, deterministic namespace so
//! the ordinary fragment actors and replication protocol can carry records for
//! multiple tables without opening another socket. This is the first 0.8
//! namespace layer; the wire protocol will grow an explicit table identity in
//! the follow-up protocol PR.

use std::time::Duration;

use actordb_query::{KeyPredicate, Projection, QueryPlan, QueryValue};

use super::{Node, NodeError};
use crate::actor::{KeyMatcher, OpPriority, ScanMatch, ScanSpec, VersionedValue};
use crate::query::QueryExecutionError;
use crate::write_concern::{WriteConcernError, WriteConcernOptions, WriteConcernReceipt};

const TABLE_PREFIX_MARKER: &str = "\u{001e}actordb.table.v1:";

/// Stable logical table identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId(String);

impl TableId {
    /// Validate and construct a table identity. Names are UTF-8 and are
    /// length-bounded like peer-visible identities.
    pub fn new(name: impl Into<String>) -> Result<Self, NodeError> {
        let name = name.into();
        if name.is_empty() || name.len() > super::MAX_WIRE_NODE_ID_BYTES {
            return Err(NodeError::Actor(format!(
                "table name must contain 1 to {} bytes",
                super::MAX_WIRE_NODE_ID_BYTES
            )));
        }
        Ok(Self(name))
    }

    /// The configured table name.
    pub fn name(&self) -> &str {
        &self.0
    }

    fn key_prefix(&self) -> String {
        format!("{TABLE_PREFIX_MARKER}{}:", hex(self.0.as_bytes()))
    }

    fn encode_key(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix(), hex(key.as_bytes()))
    }

    fn encode_prefix(&self, prefix: &[u8]) -> Vec<u8> {
        format!("{}{}", self.key_prefix(), hex(prefix)).into_bytes()
    }

    fn decode_key(&self, key: &str) -> Result<String, QueryExecutionError> {
        let prefix = self.key_prefix();
        let encoded = key
            .strip_prefix(&prefix)
            .ok_or_else(|| QueryExecutionError::Protocol("table key namespace mismatch".into()))?;
        let bytes = decode_hex(encoded).map_err(QueryExecutionError::Protocol)?;
        String::from_utf8(bytes)
            .map_err(|_| QueryExecutionError::Protocol("table key is not UTF-8".into()))
    }
}

/// A borrowed view of one logical table hosted by a [`Node`].
pub struct Table<'a> {
    node: &'a Node,
    id: TableId,
}

impl<'a> Table<'a> {
    pub(crate) fn new(node: &'a Node, id: TableId) -> Self {
        Self { node, id }
    }

    /// The table identity.
    pub fn id(&self) -> &TableId {
        &self.id
    }

    /// Write a value into this table.
    pub async fn put(&self, key: impl Into<String>, value: Vec<u8>) -> Result<(), NodeError> {
        self.node.put(self.id.encode_key(&key.into()), value).await
    }

    /// Write with TTL and scheduling priority.
    pub async fn put_with_options(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        self.node
            .put_with_options(self.id.encode_key(&key.into()), value, ttl, priority)
            .await
    }

    /// Write with a replicated acknowledgement policy.
    pub async fn put_with_write_concern_options(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        priority: OpPriority,
        options: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernError> {
        self.node
            .put_with_write_concern_options(
                self.id.encode_key(&key.into()),
                value,
                ttl,
                priority,
                options,
            )
            .await
    }

    /// Delete a key from this table.
    pub async fn delete(&self, key: impl Into<String>) -> Result<(), NodeError> {
        self.delete_with_priority(key, OpPriority::Default).await
    }

    /// Delete at an explicit scheduling priority.
    pub async fn delete_with_priority(
        &self,
        key: impl Into<String>,
        priority: OpPriority,
    ) -> Result<(), NodeError> {
        self.node
            .delete_with_priority(self.id.encode_key(&key.into()), priority)
            .await
    }

    /// Delete with a replicated acknowledgement policy.
    pub async fn delete_with_write_concern_options(
        &self,
        key: impl Into<String>,
        priority: OpPriority,
        options: WriteConcernOptions,
    ) -> Result<WriteConcernReceipt, WriteConcernError> {
        self.node
            .delete_with_write_concern_options(self.id.encode_key(&key.into()), priority, options)
            .await
    }

    /// Read a key from this table.
    pub async fn get(&self, key: impl Into<String>) -> Result<Option<VersionedValue>, NodeError> {
        self.get_with_priority(key, OpPriority::Default).await
    }

    /// Read at an explicit scheduling priority.
    pub async fn get_with_priority(
        &self,
        key: impl Into<String>,
        priority: OpPriority,
    ) -> Result<Option<VersionedValue>, NodeError> {
        self.node
            .get_with_priority(self.id.encode_key(&key.into()), priority)
            .await
    }

    /// Apply multiple writes while retaining input order in the result.
    pub async fn put_multi(&self, values: Vec<(String, Vec<u8>)>) -> Vec<Result<(), NodeError>> {
        self.put_multi_with_priority(values, OpPriority::Background)
            .await
    }

    /// Apply multiple writes at an explicit scheduling priority.
    pub async fn put_multi_with_priority(
        &self,
        values: Vec<(String, Vec<u8>)>,
        priority: OpPriority,
    ) -> Vec<Result<(), NodeError>> {
        self.node
            .put_multi_with_priority(
                values
                    .into_iter()
                    .map(|(key, value)| (self.id.encode_key(&key), value))
                    .collect(),
                priority,
            )
            .await
    }

    /// Read multiple keys while retaining input order in the result.
    pub async fn get_multi(
        &self,
        keys: Vec<String>,
    ) -> Vec<Result<Option<VersionedValue>, NodeError>> {
        self.get_multi_with_priority(keys, OpPriority::Background)
            .await
    }

    /// Read multiple keys at an explicit scheduling priority.
    pub async fn get_multi_with_priority(
        &self,
        keys: Vec<String>,
        priority: OpPriority,
    ) -> Vec<Result<Option<VersionedValue>, NodeError>> {
        self.node
            .get_multi_with_priority(
                keys.into_iter()
                    .map(|key| self.id.encode_key(&key))
                    .collect(),
                priority,
            )
            .await
    }

    /// Execute SQL scoped to this table.
    pub async fn query(
        &self,
        sql: &str,
    ) -> Result<actordb_query::QueryResult, QueryExecutionError> {
        let plan = actordb_query::parse(sql)?;
        self.query_plan(&plan).await
    }

    /// Execute parameterized SQL scoped to this table.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: &[QueryValue],
    ) -> Result<actordb_query::QueryResult, QueryExecutionError> {
        let plan = actordb_query::parse_with_params(sql, params)?;
        self.query_plan(&plan).await
    }

    /// Execute a validated plan scoped to this table.
    pub async fn query_plan(
        &self,
        plan: &QueryPlan,
    ) -> Result<actordb_query::QueryResult, QueryExecutionError> {
        let scoped = self.namespace_plan(plan)?;
        let mut result = self.node.query_plan(&scoped).await?;
        for (projection_index, projection) in scoped.projections.iter().enumerate() {
            if !matches!(projection, Projection::Key) {
                continue;
            }
            for row in &mut result.rows {
                let Some(QueryValue::Text(key)) = row.get_mut(projection_index) else {
                    return Err(QueryExecutionError::Protocol(
                        "table key projection has an invalid cell".into(),
                    ));
                };
                *key = self.id.decode_key(key)?;
            }
        }
        Ok(result)
    }

    /// Scan visible rows in this table.
    pub async fn scan(&self, spec: &ScanSpec) -> Result<Vec<ScanMatch>, crate::node::ScanError> {
        let mut scoped = spec.clone();
        scoped.matcher = match &spec.matcher {
            KeyMatcher::All => KeyMatcher::Prefix(self.id.key_prefix().into_bytes()),
            KeyMatcher::Prefix(prefix) => KeyMatcher::Prefix(self.id.encode_prefix(prefix)),
        };
        let mut rows = self.node.scan(&scoped).await?;
        for row in &mut rows {
            row.key = self
                .id
                .decode_key(&row.key)
                .map_err(|error| crate::node::ScanError::Actor(error.to_string()))?;
        }
        Ok(rows)
    }

    fn namespace_plan(&self, plan: &QueryPlan) -> Result<QueryPlan, QueryExecutionError> {
        let mut scoped = plan.clone();
        scoped.predicates = plan
            .predicates
            .iter()
            .map(|predicate| match predicate {
                KeyPredicate::Equal(key) => KeyPredicate::Equal(self.id.encode_key(key)),
                KeyPredicate::Prefix(prefix) => KeyPredicate::Prefix(self.id.encode_prefix(prefix)),
                KeyPredicate::LessThan(key) => KeyPredicate::LessThan(self.id.encode_key(key)),
                KeyPredicate::LessOrEqual(key) => {
                    KeyPredicate::LessOrEqual(self.id.encode_key(key))
                }
                KeyPredicate::GreaterThan(key) => {
                    KeyPredicate::GreaterThan(self.id.encode_key(key))
                }
                KeyPredicate::GreaterOrEqual(key) => {
                    KeyPredicate::GreaterOrEqual(self.id.encode_key(key))
                }
            })
            .collect();
        scoped
            .predicates
            .push(KeyPredicate::Prefix(self.id.key_prefix().into_bytes()));
        scoped.validate().map_err(QueryExecutionError::from)?;
        Ok(scoped)
    }
}

impl Node {
    /// Open a logical table view over this node. All table views share this
    /// node's listener, peer connections, budgets, and durable backend.
    pub fn table(&self, name: impl Into<String>) -> Result<Table<'_>, NodeError> {
        Ok(Table::new(self, TableId::new(name)?))
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_hex(encoded: &str) -> Result<Vec<u8>, String> {
    let bytes = encoded.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err("table key namespace has odd-length hex".into());
    }
    bytes
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok(((high << 4) | low) as u8)
        })
        .collect()
}

fn hex_digit(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("table key namespace has invalid hex".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_keys_are_injective_and_prefix_scoped() {
        let users = TableId::new("users").unwrap();
        let events = TableId::new("events").unwrap();
        assert_ne!(users.encode_key("a"), events.encode_key("a"));
        assert_ne!(users.encode_key("a"), users.encode_key("a\0"));
        assert_eq!(users.decode_key(&users.encode_key("a\0")).unwrap(), "a\0");
        let prefix = String::from_utf8(users.encode_prefix(b"")).unwrap();
        assert!(users.encode_key("a").starts_with(&prefix));
    }

    #[test]
    fn table_prefix_encoding_preserves_binary_prefix_matching() {
        let table = TableId::new("binary").unwrap();
        let encoded = table.encode_prefix(&[0, 0xff]);
        assert!(table
            .encode_key("\0\u{ff}")
            .as_bytes()
            .starts_with(&encoded));
    }
}
