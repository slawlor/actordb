//! Logical table views over one node.
//!
//! A table view keeps the existing node, peer listener, and replication
//! channels while using an independent per-table fragment cache and durable
//! store partition. Physical replication keys retain a deterministic table
//! namespace, but fragment routing hashes only the user key so equal join keys
//! co-partition across tables.

use std::time::Duration;

use actordb_query::{KeyPredicate, Projection, QueryPlan, QueryValue};

use super::{Node, NodeError};
use crate::actor::{KeyMatcher, OpPriority, ScanMatch, ScanSpec, VersionedValue};
use crate::query::QueryExecutionError;
use crate::write_concern::{WriteConcernError, WriteConcernOptions, WriteConcernReceipt};

/// Stable logical table identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId(String);

impl TableId {
    /// Validate and construct a table identity. Names are UTF-8 and are
    /// length-bounded like peer-visible identities.
    pub fn new(name: impl Into<String>) -> Result<Self, NodeError> {
        let name = name.into().to_ascii_uppercase();
        let mut chars = name.chars();
        let valid_identifier = chars
            .next()
            .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
            && chars.all(|character| character == '_' || character.is_ascii_alphanumeric());
        if !valid_identifier || name.len() > super::MAX_WIRE_NODE_ID_BYTES {
            return Err(NodeError::Actor(format!(
                "table name must be a SQL identifier of 1 to {} bytes",
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
        crate::table_key::table_prefix(self.name())
    }

    fn encode_key(&self, key: &str) -> String {
        crate::table_key::encode_table_key(self.name(), key)
    }

    fn encode_prefix(&self, prefix: &[u8]) -> Vec<u8> {
        crate::table_key::encode_table_prefix(self.name(), prefix)
    }

    fn decode_key(&self, key: &str) -> Result<String, QueryExecutionError> {
        crate::table_key::decode_table_key(self.name(), key)
            .ok_or_else(|| QueryExecutionError::Protocol("table key namespace mismatch".into()))
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
        self.node
            .put_qualified_with_options(
                self.id.encode_key(&key.into()),
                value,
                None,
                OpPriority::Default,
            )
            .await
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
            .put_qualified_with_options(self.id.encode_key(&key.into()), value, ttl, priority)
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
            .put_qualified_with_write_concern_options(
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
            .delete_qualified_with_priority(self.id.encode_key(&key.into()), priority)
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
            .delete_qualified_with_write_concern_options(
                self.id.encode_key(&key.into()),
                priority,
                options,
            )
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
            .get_qualified_with_priority(self.id.encode_key(&key.into()), priority)
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
            .put_multi_qualified_with_priority(
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
            .get_multi_qualified_with_priority(
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
        self.query_plan_with_options(plan, crate::query::QueryOptions::default())
            .await
    }

    pub(crate) async fn query_plan_with_options(
        &self,
        plan: &QueryPlan,
        options: crate::query::QueryOptions,
    ) -> Result<actordb_query::QueryResult, QueryExecutionError> {
        let scoped = self.namespace_plan(plan)?;
        let mut result = self
            .node
            .query_plan_with_options_scoped(&scoped, options)
            .await?;
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
        let mut rows = self.node.scan_qualified(self.id.name(), &scoped).await?;
        for row in &mut rows {
            row.key = self
                .id
                .decode_key(&row.key)
                .map_err(|error| crate::node::ScanError::Actor(error.to_string()))?;
        }
        Ok(rows)
    }

    fn namespace_plan(&self, plan: &QueryPlan) -> Result<QueryPlan, QueryExecutionError> {
        plan.validate()?;
        let mut scoped = plan.clone();
        if !scoped.table.eq_ignore_ascii_case(self.id.name()) {
            return Err(QueryExecutionError::InvalidQuery(format!(
                "SQL table {} does not match table handle {}",
                scoped.table,
                self.id.name()
            )));
        }
        scoped.table = self.id.name().into();
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
        crate::query::validate_physical_plan(&scoped)?;
        Ok(scoped)
    }
}

impl Node {
    /// Open a logical table view over this node. Table views share this node's
    /// listener, peer connections, and placement map, but use independent
    /// per-table cache/flush/iteration partitions over the backend.
    pub fn table(&self, name: impl Into<String>) -> Result<Table<'_>, NodeError> {
        Ok(Table::new(self, TableId::new(name)?))
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
        let encoded = table.encode_prefix("\0\u{ff}".as_bytes());
        assert!(table
            .encode_key("\0\u{ff}")
            .as_bytes()
            .starts_with(&encoded));
    }
}
