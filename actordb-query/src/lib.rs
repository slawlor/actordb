// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Portable query plans for named key/value relations: UTF-8 keys, opaque
//! values, and explicit durable-record metadata. `FROM kv` addresses the
//! legacy unnamed keyspace; named tables use `FROM table_name`.
//!
//! SQL is a deliberately bounded subset, with key predicates, fragment scopes,
//! projections, and mergeable integer aggregates. It does not execute Rust code
//! or infer a value schema. Query execution belongs to the database; this crate
//! owns parsing, validation, projection, and exact aggregate composition.
//!
//! ```
//! use actordb_query::{parse_with_params, QueryValue};
//! let plan = parse_with_params(
//!     "SELECT COUNT(*), SUM(OCTET_LENGTH(value)) FROM kv WHERE STARTS_WITH(key, ?)",
//!     &[QueryValue::Text("user:".into())],
//! ).unwrap();
//! assert!(plan.is_aggregate());
//! ```

mod parser;

use std::fmt;

pub use parser::{parse, parse_with_params};

/// Maximum SQL text size accepted by the parser, measured in UTF-8 bytes.
pub const MAX_SQL_BYTES: usize = 65_536;
/// Maximum lexical tokens in one statement.
pub const MAX_QUERY_TOKENS: usize = 4_096;
/// Maximum projections or key predicates in one plan.
pub const MAX_QUERY_EXPRESSIONS: usize = 32;
/// Maximum fragment IDs in an explicit query scope.
pub const MAX_QUERY_FRAGMENTS: usize = 1_024;
/// Maximum bytes in one key or prefix literal, including bound parameters.
pub const MAX_QUERY_LITERAL_BYTES: usize = 16_384;
/// Maximum explicit global row limit. Omitted limits request complete results.
pub const MAX_QUERY_LIMIT: u64 = 100_000;
/// Maximum global offset. Keyset pagination is preferable for large extracts.
pub const MAX_QUERY_OFFSET: u64 = 100_000;

/// A portable cell. Integers must travel on language-neutral wires as decimal
/// strings rather than floating-point JSON numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryValue {
    Null,
    Text(String),
    Bytes(Vec<u8>),
    Integer(i128),
}

/// Projected column names and rows, in query projection order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryValue>>,
}

/// A predefined aggregate with an associative partial-state merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
}

/// Numeric inputs are explicit. `BigInt` parses value bytes as strict UTF-8
/// decimal signed 64-bit integers; it never coerces whitespace or JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateInput {
    All,
    OctetLength,
    BigInt,
}

/// A column projection or an aggregate projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    Key,
    Value,
    Fragment,
    /// Last-writer physical timestamp from the durable record envelope.
    TimestampMs,
    /// Last-writer logical counter from the durable record envelope.
    LogicalCounter,
    /// Writer/provenance identity from the durable record envelope.
    NodeId,
    /// Optional absolute TTL deadline from the durable record envelope.
    ExpiresAtMs,
    /// Tombstone bit from the durable record envelope, represented as 0/1.
    Tombstone,
    PrefixBytes(u32),
    OctetLength,
    Aggregate {
        function: AggregateFunction,
        input: AggregateInput,
    },
}

/// Metadata carried by ActorDB's durable/replicated record envelope.
///
/// The application payload remains opaque bytes; these fields are exposed so
/// SQL can inspect the database's own record metadata without decoding the
/// payload format.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordMetadata {
    pub timestamp_ms: u64,
    pub logical_counter: u64,
    pub node_id: String,
    pub expires_at_ms: Option<u64>,
    pub is_tombstone: bool,
}

/// Comparison operators available for durable-record metadata predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataOp {
    Equal,
    LessThan,
    LessOrEqual,
    GreaterThan,
    GreaterOrEqual,
}

/// Predicate over a field in ActorDB's durable record envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataPredicate {
    TimestampMs(MetadataOp, u64),
    LogicalCounter(MetadataOp, u64),
    NodeId(MetadataOp, String),
    ExpiresAtMs(MetadataOp, u64),
    ExpiresAtMsNull(bool),
    Tombstone(MetadataOp, bool),
}

/// Conjunctive key filters. Ordering is lexicographic UTF-8 byte ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPredicate {
    Equal(String),
    Prefix(Vec<u8>),
    LessThan(String),
    LessOrEqual(String),
    GreaterThan(String),
    GreaterOrEqual(String),
}

/// A portable, validated query. Row results always use global key ascending
/// order. Fragment scopes select execution fragments, not rows after a scan.
///
/// `limit: None` requests complete results, subject to explicit executor errors
/// on resource exhaustion. `Some(0)` requests no rows. Aggregates reject LIMIT,
/// OFFSET, and ORDER BY and consume every qualifying row.
/// `include_tombstones` is false for ordinary reads; the SQL `INCLUDING
/// TOMBSTONES` modifier enables retained tombstone inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    /// SQL relation name. `KV` is the legacy unnamed keyspace; named tables
    /// are case-insensitive SQL identifiers.
    pub table: String,
    pub projections: Vec<Projection>,
    pub predicates: Vec<KeyPredicate>,
    pub metadata_predicates: Vec<MetadataPredicate>,
    pub fragments: Option<Vec<u32>>,
    pub limit: Option<u64>,
    pub offset: u64,
    pub include_tombstones: bool,
}

/// Parsing, validation, conversion, or aggregate failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    Parse {
        offset: usize,
        message: String,
    },
    InvalidPlan(String),
    InvalidValue(String),
    Overflow,
    ResourceLimit {
        resource: &'static str,
        limit: usize,
    },
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse { offset, message } => {
                write!(f, "query parse error at byte {offset}: {message}")
            }
            Self::InvalidPlan(message) => write!(f, "invalid query plan: {message}"),
            Self::InvalidValue(message) => write!(f, "invalid query value: {message}"),
            Self::Overflow => write!(f, "query integer overflow"),
            Self::ResourceLimit { resource, limit } => {
                write!(f, "query {resource} exceeds limit {limit}")
            }
        }
    }
}

impl std::error::Error for QueryError {}

impl QueryPlan {
    /// Validate a programmatically constructed plan before executing it.
    pub fn validate(&self) -> Result<(), QueryError> {
        if !valid_table_name(&self.table) {
            return Err(QueryError::InvalidPlan(
                "table must be a non-empty SQL identifier".into(),
            ));
        }
        if self.projections.is_empty() {
            return Err(QueryError::InvalidPlan(
                "at least one projection is required".into(),
            ));
        }
        check_len("projections", self.projections.len(), MAX_QUERY_EXPRESSIONS)?;
        check_len(
            "key predicates",
            self.predicates.len(),
            MAX_QUERY_EXPRESSIONS,
        )?;
        check_len(
            "metadata predicates",
            self.metadata_predicates.len(),
            MAX_QUERY_EXPRESSIONS,
        )?;
        check_len(
            "predicates",
            self.predicates
                .len()
                .saturating_add(self.metadata_predicates.len()),
            MAX_QUERY_EXPRESSIONS,
        )?;
        if let Some(fragments) = &self.fragments {
            check_len("fragments", fragments.len(), MAX_QUERY_FRAGMENTS)?;
        }
        for predicate in &self.predicates {
            let len = match predicate {
                KeyPredicate::Prefix(prefix) => prefix.len(),
                KeyPredicate::Equal(key)
                | KeyPredicate::LessThan(key)
                | KeyPredicate::LessOrEqual(key)
                | KeyPredicate::GreaterThan(key)
                | KeyPredicate::GreaterOrEqual(key) => key.len(),
            };
            check_len("literal bytes", len, MAX_QUERY_LITERAL_BYTES)?;
        }
        for predicate in &self.metadata_predicates {
            if let MetadataPredicate::NodeId(_, node_id) = predicate {
                check_len("literal bytes", node_id.len(), MAX_QUERY_LITERAL_BYTES)?;
            }
        }
        let aggregate_count = self
            .projections
            .iter()
            .filter(|p| matches!(p, Projection::Aggregate { .. }))
            .count();
        if aggregate_count != 0 && aggregate_count != self.projections.len() {
            return Err(QueryError::InvalidPlan(
                "aggregate and row projections cannot be mixed".into(),
            ));
        }
        for projection in &self.projections {
            if let Projection::Aggregate { function, input } = projection {
                let valid = match function {
                    AggregateFunction::Count => *input == AggregateInput::All,
                    _ => *input != AggregateInput::All,
                };
                if !valid {
                    return Err(QueryError::InvalidPlan(
                        "COUNT requires *; SUM/MIN/MAX require a numeric input".into(),
                    ));
                }
            }
        }
        if self.is_aggregate() && (self.limit.is_some() || self.offset != 0) {
            return Err(QueryError::InvalidPlan(
                "aggregates cannot have LIMIT or OFFSET".into(),
            ));
        }
        if self.limit.is_some_and(|limit| limit > MAX_QUERY_LIMIT) {
            return Err(QueryError::ResourceLimit {
                resource: "LIMIT",
                limit: MAX_QUERY_LIMIT as usize,
            });
        }
        if self.offset > MAX_QUERY_OFFSET {
            return Err(QueryError::ResourceLimit {
                resource: "OFFSET",
                limit: MAX_QUERY_OFFSET as usize,
            });
        }
        // Parameter binding can expand tiny placeholder SQL into a large
        // literal statement. Keep every accepted plan transmissible through
        // the same bounded parser used by other processes and languages.
        check_len("canonical SQL bytes", self.to_sql().len(), MAX_SQL_BYTES)?;
        Ok(())
    }

    pub fn is_aggregate(&self) -> bool {
        self.projections
            .iter()
            .any(|p| matches!(p, Projection::Aggregate { .. }))
    }

    /// Whether execution needs to materialize durable-record metadata.
    pub fn needs_metadata(&self) -> bool {
        !self.metadata_predicates.is_empty()
            || self.projections.iter().any(|projection| {
                matches!(
                    projection,
                    Projection::TimestampMs
                        | Projection::LogicalCounter
                        | Projection::NodeId
                        | Projection::ExpiresAtMs
                        | Projection::Tombstone
                )
            })
    }

    pub fn matches_key(&self, key: &str) -> bool {
        self.predicates.iter().all(|predicate| match predicate {
            KeyPredicate::Equal(other) => key == other,
            KeyPredicate::Prefix(prefix) => key.as_bytes().starts_with(prefix),
            KeyPredicate::LessThan(other) => key < other.as_str(),
            KeyPredicate::LessOrEqual(other) => key <= other.as_str(),
            KeyPredicate::GreaterThan(other) => key > other.as_str(),
            KeyPredicate::GreaterOrEqual(other) => key >= other.as_str(),
        })
    }

    /// Evaluate durable-record metadata predicates for one visible or
    /// tombstone record.
    pub fn matches_metadata(&self, metadata: &RecordMetadata) -> bool {
        self.metadata_predicates
            .iter()
            .all(|predicate| predicate.matches(metadata))
    }

    pub fn matches_fragment(&self, fragment: u32) -> bool {
        self.fragments
            .as_ref()
            .is_none_or(|fragments| fragments.contains(&fragment))
    }

    /// An equality usable for direct fragment routing; all other predicates
    /// must still be evaluated on the selected key.
    pub fn exact_key(&self) -> Option<&str> {
        self.predicates
            .iter()
            .find_map(|predicate| match predicate {
                KeyPredicate::Equal(key) => Some(key.as_str()),
                _ => None,
            })
    }

    pub fn columns(&self) -> Vec<String> {
        self.projections.iter().map(Projection::sql).collect()
    }

    /// Project one qualifying row. Filtering and global pagination are the
    /// executor's responsibility; this method does not apply either.
    pub fn project(
        &self,
        key: &str,
        value: &[u8],
        fragment: u32,
    ) -> Result<Vec<QueryValue>, QueryError> {
        self.project_with_metadata(key, value, fragment, &RecordMetadata::default())
    }

    /// Project one qualifying row, including durable-record metadata.
    pub fn project_with_metadata(
        &self,
        key: &str,
        value: &[u8],
        fragment: u32,
        metadata: &RecordMetadata,
    ) -> Result<Vec<QueryValue>, QueryError> {
        self.projections
            .iter()
            .map(|projection| match projection {
                Projection::Key => Ok(QueryValue::Text(key.into())),
                Projection::Value => Ok(QueryValue::Bytes(value.into())),
                Projection::Fragment => Ok(QueryValue::Integer(i128::from(fragment))),
                Projection::TimestampMs => {
                    Ok(QueryValue::Integer(i128::from(metadata.timestamp_ms)))
                }
                Projection::LogicalCounter => {
                    Ok(QueryValue::Integer(i128::from(metadata.logical_counter)))
                }
                Projection::NodeId => Ok(QueryValue::Text(metadata.node_id.clone())),
                Projection::ExpiresAtMs => {
                    Ok(metadata.expires_at_ms.map_or(QueryValue::Null, |value| {
                        QueryValue::Integer(i128::from(value))
                    }))
                }
                Projection::Tombstone => {
                    Ok(QueryValue::Integer(i128::from(metadata.is_tombstone as u8)))
                }
                Projection::PrefixBytes(count) => Ok(QueryValue::Bytes(
                    value[..value.len().min(*count as usize)].into(),
                )),
                Projection::OctetLength => Ok(QueryValue::Integer(value.len() as i128)),
                Projection::Aggregate { .. } => Err(QueryError::InvalidPlan(
                    "aggregate projections require QueryAccumulator".into(),
                )),
            })
            .collect()
    }

    /// Canonical literal SQL for transmission without another serialization
    /// dependency. Validate manually constructed plans before calling this.
    pub fn to_sql(&self) -> String {
        let mut sql = format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table.to_ascii_lowercase()
        );
        if self.include_tombstones {
            sql.push_str(" INCLUDING TOMBSTONES");
        }
        let mut predicates: Vec<String> = self
            .predicates
            .iter()
            .map(|predicate| match predicate {
                KeyPredicate::Equal(key) => format!("key = {}", quote(key)),
                KeyPredicate::Prefix(prefix) => {
                    format!("STARTS_WITH(key, {})", hex_literal(prefix))
                }
                KeyPredicate::LessThan(key) => format!("key < {}", quote(key)),
                KeyPredicate::LessOrEqual(key) => format!("key <= {}", quote(key)),
                KeyPredicate::GreaterThan(key) => format!("key > {}", quote(key)),
                KeyPredicate::GreaterOrEqual(key) => format!("key >= {}", quote(key)),
            })
            .collect();
        predicates.extend(self.metadata_predicates.iter().map(MetadataPredicate::sql));
        if let Some(fragments) = &self.fragments {
            let mut fragments = fragments.clone();
            fragments.sort_unstable();
            fragments.dedup();
            predicates.push(format!(
                "frag IN ({})",
                fragments
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !predicates.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&predicates.join(" AND "));
        }
        if !self.is_aggregate() {
            sql.push_str(" ORDER BY key ASC");
            if let Some(limit) = self.limit {
                sql.push_str(&format!(" LIMIT {limit}"));
            }
            if self.offset != 0 {
                sql.push_str(&format!(" OFFSET {}", self.offset));
            }
        }
        sql
    }
}

impl Projection {
    fn sql(&self) -> String {
        match self {
            Self::Key => "key".into(),
            Self::Value => "value".into(),
            Self::Fragment => "frag".into(),
            Self::TimestampMs => "timestamp_ms".into(),
            Self::LogicalCounter => "logical_counter".into(),
            Self::NodeId => "node_id".into(),
            Self::ExpiresAtMs => "expires_at_ms".into(),
            Self::Tombstone => "is_tombstone".into(),
            Self::PrefixBytes(count) => format!("PREFIX_BYTES(value, {count})"),
            Self::OctetLength => "OCTET_LENGTH(value)".into(),
            Self::Aggregate { function, input } => {
                let function = match function {
                    AggregateFunction::Count => "COUNT",
                    AggregateFunction::Sum => "SUM",
                    AggregateFunction::Min => "MIN",
                    AggregateFunction::Max => "MAX",
                };
                let input = match input {
                    AggregateInput::All => "*",
                    AggregateInput::OctetLength => "OCTET_LENGTH(value)",
                    AggregateInput::BigInt => "CAST(value AS BIGINT)",
                };
                format!("{function}({input})")
            }
        }
    }
}

impl MetadataPredicate {
    fn matches(&self, metadata: &RecordMetadata) -> bool {
        match self {
            Self::TimestampMs(op, value) => op.matches(metadata.timestamp_ms, *value),
            Self::LogicalCounter(op, value) => op.matches(metadata.logical_counter, *value),
            Self::NodeId(op, value) => op.matches_text(&metadata.node_id, value),
            Self::ExpiresAtMs(op, value) => metadata
                .expires_at_ms
                .is_some_and(|actual| op.matches(actual, *value)),
            Self::ExpiresAtMsNull(expected) => metadata.expires_at_ms.is_none() == *expected,
            Self::Tombstone(op, value) => op.matches(metadata.is_tombstone, *value),
        }
    }

    fn sql(&self) -> String {
        match self {
            Self::TimestampMs(op, value) => format!("timestamp_ms {} {value}", op.sql()),
            Self::LogicalCounter(op, value) => format!("logical_counter {} {value}", op.sql()),
            Self::NodeId(op, value) => format!("node_id {} {}", op.sql(), quote(value)),
            Self::ExpiresAtMs(op, value) => format!("expires_at_ms {} {value}", op.sql()),
            Self::ExpiresAtMsNull(true) => "expires_at_ms IS NULL".into(),
            Self::ExpiresAtMsNull(false) => "expires_at_ms IS NOT NULL".into(),
            Self::Tombstone(op, value) => {
                format!("is_tombstone {} {}", op.sql(), u8::from(*value))
            }
        }
    }
}

impl MetadataOp {
    fn matches<T: Ord>(self, left: T, right: T) -> bool {
        match self {
            Self::Equal => left == right,
            Self::LessThan => left < right,
            Self::LessOrEqual => left <= right,
            Self::GreaterThan => left > right,
            Self::GreaterOrEqual => left >= right,
        }
    }

    fn matches_text(self, left: &str, right: &str) -> bool {
        self.matches(left, right)
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::LessThan => "<",
            Self::LessOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterOrEqual => ">=",
        }
    }
}

#[derive(Debug, Clone)]
enum AggregateState {
    Count(u64),
    Number(Option<i128>),
    Unsupported,
}

/// Incremental aggregate execution. Partial results can be merged without
/// transferring underlying values. Executors must merge every fragment/range
/// once and finalize only after receiving explicit completion from all scopes.
#[derive(Debug, Clone)]
pub struct QueryAccumulator {
    plan: QueryPlan,
    states: Vec<AggregateState>,
    validation_error: Option<QueryError>,
}

impl QueryAccumulator {
    pub fn new(plan: &QueryPlan) -> Self {
        let states = plan
            .projections
            .iter()
            .map(|projection| match projection {
                Projection::Aggregate {
                    function: AggregateFunction::Count,
                    ..
                } => AggregateState::Count(0),
                Projection::Aggregate { .. } => AggregateState::Number(None),
                _ => AggregateState::Unsupported,
            })
            .collect();
        Self {
            plan: plan.clone(),
            states,
            validation_error: plan.validate().err().or_else(|| {
                (!plan.is_aggregate()).then(|| {
                    QueryError::InvalidPlan(
                        "QueryAccumulator requires aggregate projections".into(),
                    )
                })
            }),
        }
    }

    /// Add one raw row, applying the plan's key and fragment filters.
    pub fn push(&mut self, key: &str, value: &[u8], fragment: u32) -> Result<(), QueryError> {
        self.push_with_metadata(key, value, fragment, &RecordMetadata::default())
    }

    /// Add one raw row with durable-record metadata, applying all predicates.
    pub fn push_with_metadata(
        &mut self,
        key: &str,
        value: &[u8],
        fragment: u32,
        metadata: &RecordMetadata,
    ) -> Result<(), QueryError> {
        self.ensure_aggregate()?;
        if !self.plan.matches_fragment(fragment)
            || !self.plan.matches_key(key)
            || !self.plan.matches_metadata(metadata)
        {
            return Ok(());
        }
        // Compute all conversions and updates before changing any state. A
        // rejected row must not increment COUNT ahead of a failing SUM cast.
        let mut updated = self.states.clone();
        for (projection, state) in self.plan.projections.iter().zip(&mut updated) {
            if let Projection::Aggregate { function, input } = projection {
                let number = match input {
                    AggregateInput::All => 1,
                    AggregateInput::OctetLength => value.len() as i128,
                    AggregateInput::BigInt => parse_bigint(value)?,
                };
                update_state(state, *function, Some(number))?;
            }
        }
        self.states = updated;
        Ok(())
    }

    /// Merge one complete partial aggregate row. This operation is atomic on
    /// conversion/overflow failure and validates schema and numeric cell types.
    pub fn merge(&mut self, partial: &QueryResult) -> Result<(), QueryError> {
        self.ensure_aggregate()?;
        if partial.columns != self.plan.columns()
            || partial.rows.len() != 1
            || partial.rows[0].len() != self.states.len()
        {
            return Err(QueryError::InvalidValue(
                "partial aggregate schema does not match the plan".into(),
            ));
        }
        let mut updated = self.states.clone();
        for ((projection, state), cell) in self
            .plan
            .projections
            .iter()
            .zip(&mut updated)
            .zip(&partial.rows[0])
        {
            let number = match cell {
                QueryValue::Null => None,
                QueryValue::Integer(number) => Some(*number),
                _ => {
                    return Err(QueryError::InvalidValue(
                        "partial aggregates require integer or null cells".into(),
                    ))
                }
            };
            if let Projection::Aggregate { function, input } = projection {
                if *input == AggregateInput::OctetLength && number.is_some_and(|n| n < 0) {
                    return Err(QueryError::InvalidValue(
                        "byte lengths cannot be negative".into(),
                    ));
                }
                if *input == AggregateInput::BigInt
                    && matches!(function, AggregateFunction::Min | AggregateFunction::Max)
                    && number.is_some_and(|n| i64::try_from(n).is_err())
                {
                    return Err(QueryError::InvalidValue(
                        "BIGINT extrema must fit signed 64-bit integers".into(),
                    ));
                }
                update_state(state, *function, number)?;
            }
        }
        self.states = updated;
        Ok(())
    }

    /// Return one aggregate row, including empty-set COUNT=0 and other nulls.
    pub fn finish(self) -> QueryResult {
        let row = self
            .states
            .into_iter()
            .map(|state| match state {
                AggregateState::Count(count) => QueryValue::Integer(i128::from(count)),
                AggregateState::Number(Some(number)) => QueryValue::Integer(number),
                AggregateState::Number(None) | AggregateState::Unsupported => QueryValue::Null,
            })
            .collect();
        QueryResult {
            columns: self.plan.columns(),
            rows: vec![row],
        }
    }

    fn ensure_aggregate(&self) -> Result<(), QueryError> {
        if let Some(error) = &self.validation_error {
            return Err(error.clone());
        }
        Ok(())
    }
}

fn update_state(
    state: &mut AggregateState,
    function: AggregateFunction,
    number: Option<i128>,
) -> Result<(), QueryError> {
    match (state, function) {
        (AggregateState::Count(count), AggregateFunction::Count) => {
            let add = number.and_then(|n| u64::try_from(n).ok()).ok_or_else(|| {
                QueryError::InvalidValue(
                    "COUNT partials must be nonnegative unsigned 64-bit integers".into(),
                )
            })?;
            *count = count.checked_add(add).ok_or(QueryError::Overflow)?;
        }
        (AggregateState::Number(current), AggregateFunction::Sum) => {
            if let Some(number) = number {
                *current = Some(
                    current
                        .unwrap_or(0)
                        .checked_add(number)
                        .ok_or(QueryError::Overflow)?,
                );
            }
        }
        (AggregateState::Number(current), AggregateFunction::Min) => {
            if let Some(number) = number {
                *current = Some(current.map_or(number, |n| n.min(number)));
            }
        }
        (AggregateState::Number(current), AggregateFunction::Max) => {
            if let Some(number) = number {
                *current = Some(current.map_or(number, |n| n.max(number)));
            }
        }
        _ => {
            return Err(QueryError::InvalidPlan(
                "aggregate function does not match its state".into(),
            ))
        }
    }
    Ok(())
}

fn parse_bigint(value: &[u8]) -> Result<i128, QueryError> {
    if value.len() > 20 {
        return Err(QueryError::InvalidValue(
            "CAST(value AS BIGINT) requires at most 20 decimal bytes including its sign".into(),
        ));
    }
    let text = std::str::from_utf8(value).map_err(|_| {
        QueryError::InvalidValue("CAST(value AS BIGINT) requires UTF-8 decimal bytes".into())
    })?;
    let digits = text.strip_prefix(['-', '+']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(QueryError::InvalidValue(
            "CAST(value AS BIGINT) requires an optional sign followed by decimal digits".into(),
        ));
    }
    text.parse::<i64>().map(i128::from).map_err(|_| {
        QueryError::InvalidValue("CAST(value AS BIGINT) is outside the signed 64-bit range".into())
    })
}

fn valid_table_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
        && name.len() <= 255
}

fn check_len(resource: &'static str, len: usize, limit: usize) -> Result<(), QueryError> {
    if len > limit {
        Err(QueryError::ResourceLimit { resource, limit })
    } else {
        Ok(())
    }
}

fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn hex_literal(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(bytes.len() * 2 + 3);
    text.push_str("X'");
    for byte in bytes {
        write!(&mut text, "{byte:02x}").expect("writing to String cannot fail");
    }
    text.push('\'');
    text
}

#[cfg(test)]
mod tests;
