//! Server coordinator for bounded, revision-guarded query pages.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use actordb_query::{QueryAccumulator, QueryPlan, QueryResult, QueryValue};
use ractor::{rpc::CallResult, ActorRef};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{
    try_send_envelope_until, FragmentRouter, Node, PeerChannel, CAPABILITY_QUERY_METADATA,
    CAPABILITY_QUERY_PAGES,
};
use crate::actor::KvMessage;
use crate::proto::{Envelope, EnvelopeMsg, QueryPageRequest, QueryPageResponse, QueryPageStatus};
use crate::query::{
    cell_to_value, page_encoded_len, page_envelope, page_error, result_row_storage, row_storage,
    QueryAdmission, QueryExecutionError, QueryOptions, QueryPageWork, QueryRetention,
    MAX_QUERY_PAGE_BYTES, QUERY_PAGE_VISITS,
};
use crate::shard::{fragment_of, FragmentId};

pub(super) type PendingQueryPages = Arc<StdMutex<HashMap<u64, PendingQueryPage>>>;
pub(super) struct PendingQueryPage {
    pub(super) channel: PeerChannel,
    pub(super) reply: oneshot::Sender<QueryPageResponse>,
}
struct PendingGuard {
    id: u64,
    pending: PendingQueryPages,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

#[derive(Clone)]
enum Holder {
    Local(ActorRef<KvMessage>),
    Remote(PeerChannel),
}
struct FragmentCursor {
    fragment: u32,
    holder: Option<Holder>,
    after_key: String,
    revision: u64,
    time: u64,
    incarnation: u64,
    initialized: bool,
    done: bool,
    rows: VecDeque<crate::proto::QueryPageRow>,
}

fn query_requires_metadata(plan: &QueryPlan) -> bool {
    plan.include_tombstones || plan.needs_metadata()
}
impl FragmentCursor {
    fn new(fragment: u32) -> Self {
        Self {
            fragment,
            holder: None,
            after_key: String::new(),
            revision: 0,
            time: 0,
            incarnation: 0,
            initialized: false,
            done: false,
            rows: VecDeque::new(),
        }
    }
    fn storage(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.after_key.capacity())
            .saturating_add(self.rows.front().map_or(0, |row| row.key.capacity()))
            .saturating_add(
                self.rows
                    .capacity()
                    .saturating_mul(std::mem::size_of::<crate::proto::QueryPageRow>()),
            )
            .saturating_add(self.rows.iter().map(row_storage).sum::<usize>())
    }
}

impl Node {
    /// Return conservative node-wide query/scan admission reservations.
    /// Returned result ownership belongs to the caller and is outside this
    /// execution allowance after the coordinator completes.
    pub fn query_diagnostics(&self) -> crate::query::QueryDiagnostics {
        self.query_admission.diagnostics()
    }

    /// Execute one SQL statement over selected fragments with global key ordering.
    /// Conjunctive equality, range, and byte-prefix predicates seek the ordered
    /// index directly. Work limits count keys within that selected range,
    /// including tombstones and expired values. Queries fail completely on
    /// resource exhaustion or a changed fragment.
    pub async fn query(&self, sql: &str) -> Result<QueryResult, QueryExecutionError> {
        let plan = actordb_query::parse(sql)?;
        self.query_plan(&plan).await
    }
    /// Parse positional parameters locally; literal values are preserved in peer requests.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: &[QueryValue],
    ) -> Result<QueryResult, QueryExecutionError> {
        let plan = actordb_query::parse_with_params(sql, params)?;
        self.query_plan(&plan).await
    }
    /// Execute a validated structured query plan using default limits.
    pub async fn query_plan(&self, plan: &QueryPlan) -> Result<QueryResult, QueryExecutionError> {
        self.query_plan_with_options(plan, QueryOptions::default())
            .await
    }
    /// Execute a query under one absolute deadline and explicit memory/work limits.
    pub async fn query_plan_with_options(
        &self,
        plan: &QueryPlan,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryExecutionError> {
        self.query_plan_with_options_and_visits(plan, options)
            .await
            .map(|(result, _)| result)
    }

    pub(super) async fn query_plan_with_options_and_visits(
        &self,
        plan: &QueryPlan,
        options: QueryOptions,
    ) -> Result<(QueryResult, u64), QueryExecutionError> {
        let _operation = self
            .admit_operation()
            .map_err(|_| QueryExecutionError::ShuttingDown)?;
        plan.validate()?;
        let deadline = Instant::now()
            .checked_add(options.timeout)
            .filter(|_| !options.timeout.is_zero())
            .ok_or_else(|| {
                QueryExecutionError::InvalidQuery(
                    "timeout must be nonzero and representable".into(),
                )
            })?;
        if options.max_result_bytes == 0
            || options.max_buffer_bytes == 0
            || options.max_scanned_rows == 0
            || !(1024..=MAX_QUERY_PAGE_BYTES).contains(&options.page_bytes)
            || !(1..=QUERY_PAGE_VISITS).contains(&options.page_visits)
        {
            return Err(QueryExecutionError::InvalidQuery(
                "invalid query budgets".into(),
            ));
        }
        let sql = plan.to_sql();
        if sql.len() > 1024 * 1024 {
            return Err(QueryExecutionError::BudgetExceeded(
                "query text exceeds 1 MiB".into(),
            ));
        }
        let retention = self.query_admission.reserve_work(
            sql.len().saturating_mul(4).saturating_add(1024),
            options
                .max_result_bytes
                .checked_add(options.max_buffer_bytes)
                .ok_or_else(|| {
                    QueryExecutionError::BudgetExceeded("query runtime reservation overflow".into())
                })?,
        )?;
        if plan.fragments.as_ref().is_some_and(|ids| {
            ids.iter()
                .any(|id| *id as usize >= self.router.fragment_count())
        }) {
            return Err(QueryExecutionError::InvalidQuery(
                "fragment scope is outside configured fragment count".into(),
            ));
        }
        let exact_fragment = plan
            .exact_key()
            .map(|key| fragment_of(key, self.router.fragment_count()).0);
        let mut scoped_fragments = plan.fragments.clone();
        if let Some(fragments) = &mut scoped_fragments {
            fragments.sort_unstable();
            fragments.dedup();
        }
        let selected_count = if let Some(exact) = exact_fragment {
            usize::from(plan.matches_fragment(exact))
        } else {
            scoped_fragments
                .as_ref()
                .map_or(self.router.fragment_count(), Vec::len)
        };
        let topology_bytes = selected_count
            .saturating_mul(std::mem::size_of::<FragmentCursor>())
            .saturating_add(sql.capacity());
        if topology_bytes > options.max_buffer_bytes {
            return Err(QueryExecutionError::BudgetExceeded(
                "selected fragment state".into(),
            ));
        }
        let mut cursors = Vec::with_capacity(selected_count);
        if let Some(exact) = exact_fragment {
            if plan.matches_fragment(exact) {
                cursors.push(FragmentCursor::new(exact));
            }
        } else if let Some(fragments) = &scoped_fragments {
            cursors.extend(fragments.iter().copied().map(FragmentCursor::new));
        } else {
            cursors
                .extend((0..self.router.fragment_count()).map(|id| FragmentCursor::new(id as u32)));
        }
        tokio::time::timeout_at(
            deadline,
            self.execute_query(
                Arc::new(plan.clone()),
                &sql,
                &mut cursors,
                &options,
                retention,
                deadline,
            ),
        )
        .await
        .map_err(|_| QueryExecutionError::Timeout)?
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_query(
        &self,
        plan: Arc<QueryPlan>,
        sql: &str,
        cursors: &mut [FragmentCursor],
        options: &QueryOptions,
        retention: Arc<QueryRetention>,
        deadline: Instant,
    ) -> Result<(QueryResult, u64), QueryExecutionError> {
        let mut result = QueryResult {
            columns: plan.columns(),
            rows: Vec::new(),
        };
        let base_bytes = std::mem::size_of::<QueryResult>()
            .saturating_add(result.columns.capacity() * std::mem::size_of::<String>())
            .saturating_add(result.columns.iter().map(String::capacity).sum::<usize>());
        if base_bytes > options.max_result_bytes {
            return Err(QueryExecutionError::BudgetExceeded("result columns".into()));
        }
        if plan.limit == Some(0) {
            return Ok((result, 0));
        }
        // Reserve heap headers before admitting pages. Cursor accounting also
        // includes the duplicated head key while the heap owns it.
        let mut merge_options = options.clone();
        if !plan.is_aggregate() {
            merge_options.max_buffer_bytes = merge_options.max_buffer_bytes.saturating_sub(
                cursors
                    .len()
                    .saturating_mul(std::mem::size_of::<Reverse<(String, usize)>>()),
            );
        }
        let options = &merge_options;
        let mut visited = 0u64;
        if plan.is_aggregate() {
            let mut aggregate = QueryAccumulator::new(&plan);
            // One active fragment page keeps aggregate memory independent of
            // record count; the accumulator merges exact partial states.
            for index in 0..cursors.len() {
                while !cursors[index].done {
                    let page_options = options_for_page(cursors, index, options, sql.len())?;
                    self.fetch_query_page(
                        &mut cursors[index],
                        &plan,
                        sql,
                        &page_options,
                        retention.clone(),
                        deadline,
                        false,
                        &mut visited,
                    )
                    .await?;
                    check_buffer(cursors, options, sql.len())?;
                    let partial = QueryResult {
                        columns: plan.columns(),
                        rows: cursors[index]
                            .rows
                            .drain(..)
                            .map(|row| row.cells.into_iter().map(cell_to_value).collect())
                            .collect::<Result<_, _>>()?,
                    };
                    aggregate.merge(&partial)?;
                }
            }
            result = aggregate.finish();
            if plan.offset > 0 {
                result.rows.clear();
            }
        } else {
            let mut heads = BinaryHeap::with_capacity(cursors.len());
            // Obtain one bounded head page from each selected fragment. No
            // fragment-wide Vec or unbounded concurrent RPC fan-out is used.
            for index in 0..cursors.len() {
                let page_options = options_for_page(cursors, index, options, sql.len())?;
                self.fill_query_head(
                    &mut cursors[index],
                    &plan,
                    sql,
                    &page_options,
                    retention.clone(),
                    deadline,
                    &mut visited,
                )
                .await?;
                check_buffer(cursors, options, sql.len())?;
                if let Some(row) = cursors[index].rows.front() {
                    heads.push(Reverse((row.key.clone(), index)));
                }
            }
            let mut skipped = 0u64;
            let mut payload_bytes = 0usize;
            while let Some(Reverse((key, index))) = heads.pop() {
                drop(key);
                let row = cursors[index]
                    .rows
                    .pop_front()
                    .expect("selected head exists");
                if skipped < plan.offset {
                    skipped += 1;
                } else {
                    let values: Vec<_> = row
                        .cells
                        .into_iter()
                        .map(cell_to_value)
                        .collect::<Result<_, _>>()?;
                    payload_bytes = payload_bytes.saturating_add(result_row_storage(&values));
                    result.rows.push(values);
                    let bytes = base_bytes.saturating_add(payload_bytes).saturating_add(
                        result.rows.capacity() * std::mem::size_of::<Vec<QueryValue>>(),
                    );
                    if bytes > options.max_result_bytes {
                        return Err(QueryExecutionError::BudgetExceeded(
                            "projected result storage".into(),
                        ));
                    }
                    if plan
                        .limit
                        .is_some_and(|limit| result.rows.len() as u64 >= limit)
                    {
                        break;
                    }
                }
                let page_options = options_for_page(cursors, index, options, sql.len())?;
                self.fill_query_head(
                    &mut cursors[index],
                    &plan,
                    sql,
                    &page_options,
                    retention.clone(),
                    deadline,
                    &mut visited,
                )
                .await?;
                check_buffer(cursors, options, sql.len())?;
                if let Some(row) = cursors[index].rows.front() {
                    heads.push(Reverse((row.key.clone(), index)));
                }
            }
        }
        // Recheck every accepted contribution, including completed fragments,
        // before releasing a successful result. This is per-fragment checking;
        // the check instants do not create a distributed atomic snapshot.
        for index in 0..cursors.len() {
            if !cursors[index].initialized {
                continue;
            }
            let page_options = options_for_page(cursors, index, options, sql.len())?;
            self.fetch_query_page(
                &mut cursors[index],
                &plan,
                sql,
                &page_options,
                retention.clone(),
                deadline,
                true,
                &mut visited,
            )
            .await?;
        }
        let bytes = base_bytes.saturating_add(
            result
                .rows
                .iter()
                .map(|row| result_row_storage(row))
                .sum::<usize>(),
        );
        if bytes > options.max_result_bytes {
            return Err(QueryExecutionError::BudgetExceeded(
                "aggregate result storage".into(),
            ));
        }
        Ok((result, visited))
    }

    #[allow(clippy::too_many_arguments)]
    async fn fill_query_head(
        &self,
        cursor: &mut FragmentCursor,
        plan: &Arc<QueryPlan>,
        sql: &str,
        options: &QueryOptions,
        retention: Arc<QueryRetention>,
        deadline: Instant,
        visited: &mut u64,
    ) -> Result<(), QueryExecutionError> {
        while cursor.rows.is_empty() && !cursor.done {
            let available = options.max_buffer_bytes.saturating_sub(cursor.storage());
            let mut page_options = options.clone();
            page_options.page_bytes = options.page_bytes.min(available.saturating_sub(4096) / 32);
            if page_options.page_bytes < 1024 {
                return Err(QueryExecutionError::BudgetExceeded(
                    "incoming query page storage".into(),
                ));
            }
            self.fetch_query_page(
                cursor,
                plan,
                sql,
                &page_options,
                retention.clone(),
                deadline,
                false,
                visited,
            )
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_query_page(
        &self,
        cursor: &mut FragmentCursor,
        plan: &Arc<QueryPlan>,
        sql: &str,
        options: &QueryOptions,
        retention: Arc<QueryRetention>,
        deadline: Instant,
        verify: bool,
        visited: &mut u64,
    ) -> Result<(), QueryExecutionError> {
        let remaining_visits = options.max_scanned_rows.saturating_sub(*visited);
        if !verify && remaining_visits == 0 {
            return Err(QueryExecutionError::BudgetExceeded(
                "visited record limit".into(),
            ));
        }
        let req = QueryPageRequest {
            req_id: self
                .next_req_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            fragment: cursor.fragment,
            sql: sql.to_string(),
            after_key: cursor.after_key.clone(),
            continuation: cursor.initialized,
            revision: cursor.revision,
            snapshot_time_ms: cursor.time,
            incarnation: cursor.incarnation,
            max_rows: 256,
            max_bytes: options.page_bytes as u32,
            max_visits: if verify {
                options.page_visits as u32
            } else {
                (options.page_visits as u64).min(remaining_visits) as u32
            },
            verify_only: verify,
        };
        let page = if let Some(holder) = cursor.holder.clone() {
            self.query_page_from_holder(
                holder,
                plan.clone(),
                req.clone(),
                retention.clone(),
                deadline,
            )
            .await
            .map_err(|error| match error {
                QueryExecutionError::FragmentsUnavailable(_) => {
                    QueryExecutionError::QueryChanged(cursor.fragment)
                }
                other => other,
            })?
        } else {
            let mut holders = Vec::new();
            let fragment = FragmentId(cursor.fragment);
            if self.serving.read().unwrap().contains(&fragment) {
                if let Some(actor) = self.router.route_for_fragment(fragment) {
                    holders.push(Holder::Local(actor));
                }
            }
            for owner in self.replica_order(fragment) {
                if owner == self.node_id {
                    continue;
                }
                if let Some(channel) = self.peers.lock().await.get(&owner).cloned() {
                    if channel.capabilities & CAPABILITY_QUERY_PAGES != 0
                        && (!query_requires_metadata(plan)
                            || channel.capabilities & CAPABILITY_QUERY_METADATA != 0)
                    {
                        holders.push(Holder::Remote(channel));
                    }
                }
            }
            let mut successful = None;
            for holder in holders {
                let attempt = self
                    .query_page_from_holder(
                        holder.clone(),
                        plan.clone(),
                        req.clone(),
                        retention.clone(),
                        deadline,
                    )
                    .await;
                match attempt {
                    Ok(page) if page.status != QueryPageStatus::Unavailable as i32 => {
                        successful = Some((holder, page));
                        break;
                    }
                    Ok(_) | Err(QueryExecutionError::FragmentsUnavailable(_)) => continue,
                    Err(error) => return Err(error),
                }
            }
            let (holder, page) = successful
                .ok_or_else(|| QueryExecutionError::FragmentsUnavailable(vec![cursor.fragment]))?;
            cursor.holder = Some(holder);
            page
        };
        validate_page(&page, &req, plan, self.router.fragment_count())?;
        match QueryPageStatus::try_from(page.status)
            .map_err(|_| QueryExecutionError::Protocol("unknown page status".into()))?
        {
            QueryPageStatus::Ok => {}
            QueryPageStatus::Changed => {
                return Err(QueryExecutionError::QueryChanged(cursor.fragment))
            }
            QueryPageStatus::Backpressure => {
                return Err(QueryExecutionError::Backpressure(page.error))
            }
            QueryPageStatus::Unavailable => {
                return Err(QueryExecutionError::QueryChanged(cursor.fragment))
            }
            QueryPageStatus::Invalid => return Err(QueryExecutionError::InvalidQuery(page.error)),
            QueryPageStatus::Budget => return Err(QueryExecutionError::BudgetExceeded(page.error)),
            QueryPageStatus::Unknown => {
                return Err(QueryExecutionError::Protocol("unknown page status".into()))
            }
        }
        if verify {
            return Ok(());
        }
        *visited = visited.saturating_add(page.visited as u64);
        if *visited > options.max_scanned_rows {
            return Err(QueryExecutionError::BudgetExceeded(
                "visited record limit".into(),
            ));
        }
        cursor.revision = page.revision;
        cursor.time = page.snapshot_time_ms;
        cursor.incarnation = page.incarnation;
        cursor.after_key = page.next_after_key;
        cursor.done = page.done;
        cursor.initialized = true;
        cursor.rows = page.rows.into();
        Ok(())
    }

    async fn query_page_from_holder(
        &self,
        holder: Holder,
        plan: Arc<QueryPlan>,
        req: QueryPageRequest,
        retention: Arc<QueryRetention>,
        deadline: Instant,
    ) -> Result<QueryPageResponse, QueryExecutionError> {
        match holder {
            Holder::Local(actor) => {
                let fragment = FragmentId(req.fragment);
                if !self.serving.read().unwrap().contains(&fragment)
                    || self
                        .router
                        .route_for_fragment(fragment)
                        .is_none_or(|current| current.get_id() != actor.get_id())
                {
                    return Err(QueryExecutionError::FragmentsUnavailable(vec![
                        req.fragment,
                    ]));
                }
                actor_page(actor, plan, req, retention, deadline).await
            }
            Holder::Remote(channel) => {
                if *channel.cancel.borrow() {
                    return Err(QueryExecutionError::FragmentsUnavailable(vec![
                        req.fragment,
                    ]));
                }
                let (reply, response) = oneshot::channel();
                self.pending_query_pages.lock().unwrap().insert(
                    req.req_id,
                    PendingQueryPage {
                        channel: channel.clone(),
                        reply,
                    },
                );
                let _guard = PendingGuard {
                    id: req.req_id,
                    pending: self.pending_query_pages.clone(),
                };
                let fragment = req.fragment;
                let attempt_deadline =
                    deadline.min(Instant::now() + self.runtime_options.forwarded_attempt_timeout);
                if !try_send_envelope_until(
                    &channel,
                    Envelope {
                        msg: Some(EnvelopeMsg::QueryPageRequest(req)),
                    },
                    attempt_deadline,
                )
                .await
                {
                    return Err(QueryExecutionError::FragmentsUnavailable(vec![fragment]));
                }
                let mut cancelled = channel.cancel.subscribe();
                tokio::select! {
                    biased;
                    _ = cancelled.wait_for(|closed| *closed) => Err(QueryExecutionError::FragmentsUnavailable(vec![fragment])),
                    response = tokio::time::timeout_at(attempt_deadline, response) => response.ok().and_then(Result::ok).ok_or_else(|| QueryExecutionError::FragmentsUnavailable(vec![fragment])),
                }
            }
        }
    }
}

fn options_for_page(
    cursors: &[FragmentCursor],
    index: usize,
    options: &QueryOptions,
    sql_bytes: usize,
) -> Result<QueryOptions, QueryExecutionError> {
    let retained = cursors
        .iter()
        .map(FragmentCursor::storage)
        .sum::<usize>()
        .saturating_add(sql_bytes);
    let available = options.max_buffer_bytes.saturating_sub(retained);
    // Minimum protobuf row encodings can amplify into String/Vec/enum headers.
    // 32 times encoded bytes plus fixed metadata conservatively covers the
    // bounded 256-row/32-column shape and temporary validation copies.
    let wire_budget = available.saturating_sub(4096) / 32;
    if wire_budget < 1024 {
        return Err(QueryExecutionError::BudgetExceeded(
            "incoming query page storage".into(),
        ));
    }
    let mut page_options = options.clone();
    page_options.page_bytes = options.page_bytes.min(wire_budget);
    // Sparse continuation pages may grow their own cursor between reads.
    page_options.max_buffer_bytes = available.saturating_add(cursors[index].storage());
    Ok(page_options)
}

fn check_buffer(
    cursors: &[FragmentCursor],
    options: &QueryOptions,
    sql_bytes: usize,
) -> Result<(), QueryExecutionError> {
    if cursors
        .iter()
        .map(FragmentCursor::storage)
        .sum::<usize>()
        .saturating_add(sql_bytes)
        > options.max_buffer_bytes
    {
        return Err(QueryExecutionError::BudgetExceeded(
            "fragment merge pages".into(),
        ));
    }
    Ok(())
}

fn validate_page(
    page: &QueryPageResponse,
    req: &QueryPageRequest,
    plan: &QueryPlan,
    fragment_count: usize,
) -> Result<(), QueryExecutionError> {
    let bad = |s: &str| QueryExecutionError::Protocol(s.into());
    if page.req_id != req.req_id
        || page.fragment != req.fragment
        || page_encoded_len(page) > req.max_bytes as usize
        || page.error.len() > 1024
        || page.visited > req.max_visits
        || page.work_bytes > crate::query::QUERY_WORK_BYTES as u64
    {
        return Err(bad("page correlation or limits"));
    }
    if page.status != QueryPageStatus::Ok as i32 {
        if !page.rows.is_empty() {
            return Err(bad("error page contains rows"));
        }
        return Ok(());
    }
    if !page.error.is_empty()
        || page.snapshot_time_ms == 0
        || page.incarnation == 0
        || (req.continuation
            && (page.revision != req.revision
                || page.incarnation != req.incarnation
                || page.snapshot_time_ms != req.snapshot_time_ms))
    {
        return Err(bad("page pinned identity/time"));
    }
    if req.verify_only {
        if !page.rows.is_empty() || page.visited != 0 || page.next_after_key != req.after_key {
            return Err(bad("verification page advanced"));
        }
        return Ok(());
    }
    if (!page.done && page.visited == 0)
        || (page.visited > 0 && req.continuation && page.next_after_key <= req.after_key)
    {
        return Err(bad("page did not advance"));
    }
    if page.visited == 0 && page.next_after_key != req.after_key {
        return Err(bad("empty page changed cursor"));
    }
    if plan.is_aggregate() {
        if page.rows.len() != 1 || !page.rows[0].key.is_empty() {
            return Err(bad("aggregate partial shape"));
        }
    } else {
        if page.rows.len() > req.max_rows as usize || page.rows.len() > page.visited as usize {
            return Err(bad("row page cardinality"));
        }
        let mut previous = req.continuation.then_some(req.after_key.as_str());
        for row in &page.rows {
            if previous.is_some_and(|key| row.key.as_str() <= key)
                || row.key > page.next_after_key
                || !plan.matches_key(&row.key)
                || fragment_of(&row.key, fragment_count).0 != req.fragment
            {
                return Err(bad("row order/cursor/predicate"));
            }
            previous = Some(&row.key);
        }
    }
    for row in &page.rows {
        let mut observed_count = None;
        if row.cells.len() != plan.projections.len() {
            return Err(bad("projected row width"));
        }
        for (projection, cell) in plan.projections.iter().zip(&row.cells) {
            use crate::proto::meta::query_cell::Value;
            use actordb_query::{AggregateFunction, Projection};
            let valid = match (projection, &cell.value) {
                (Projection::Key, Some(Value::Text(value))) => value == &row.key,
                (Projection::Value, Some(Value::Bytes(_))) => true,
                (Projection::PrefixBytes(n), Some(Value::Bytes(value))) => {
                    value.len() <= *n as usize
                }
                (Projection::Fragment, Some(Value::Integer(value))) => {
                    value == &req.fragment.to_string()
                }
                (
                    Projection::TimestampMs | Projection::LogicalCounter,
                    Some(Value::Integer(value)),
                ) => value.parse::<u64>().is_ok(),
                (Projection::NodeId, Some(Value::Text(value))) => {
                    !value.is_empty() && value.len() <= 255
                }
                (Projection::ExpiresAtMs, Some(Value::Null(true) | Value::Integer(_))) => {
                    match &cell.value {
                        Some(Value::Null(true)) => true,
                        Some(Value::Integer(value)) => value.parse::<u64>().is_ok(),
                        _ => false,
                    }
                }
                (Projection::Tombstone, Some(Value::Integer(value))) => {
                    value == "0" || value == "1"
                }
                (Projection::OctetLength, Some(Value::Integer(value))) => {
                    value.parse::<u64>().is_ok()
                }
                (
                    Projection::Aggregate {
                        function: AggregateFunction::Count,
                        ..
                    },
                    Some(Value::Integer(value)),
                ) => value
                    .parse::<u64>()
                    .is_ok_and(|count| count <= page.visited as u64),
                (
                    Projection::Aggregate {
                        function:
                            AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max,
                        ..
                    },
                    Some(Value::Integer(_) | Value::Null(true)),
                ) => true,
                _ => false,
            };
            if !valid {
                return Err(bad("projected cell type/value"));
            }
            let decoded = cell_to_value(cell.clone())?;
            if matches!(
                projection,
                actordb_query::Projection::Aggregate {
                    function: actordb_query::AggregateFunction::Count,
                    ..
                }
            ) {
                if observed_count
                    .as_ref()
                    .is_some_and(|previous| previous != &decoded)
                {
                    return Err(bad("inconsistent repeated COUNT partials"));
                }
                observed_count = Some(decoded);
            }
        }
    }
    Ok(())
}

async fn actor_page(
    actor: ActorRef<KvMessage>,
    plan: Arc<QueryPlan>,
    request: QueryPageRequest,
    retention: Arc<QueryRetention>,
    deadline: Instant,
) -> Result<QueryPageResponse, QueryExecutionError> {
    let fragment = request.fragment;
    let work = QueryPageWork {
        plan,
        request,
        retention: Some(retention),
    };
    match tokio::time::timeout_at(
        deadline,
        actor.call(|reply| KvMessage::QueryPage { work, reply }, None),
    )
    .await
    {
        Ok(Ok(CallResult::Success(page))) => Ok(page),
        _ => Err(QueryExecutionError::FragmentsUnavailable(vec![fragment])),
    }
}

pub(super) async fn serve_query_page(
    channel: PeerChannel,
    router: FragmentRouter,
    serving: Arc<std::sync::RwLock<std::collections::HashSet<FragmentId>>>,
    request: QueryPageRequest,
    retention: Arc<QueryRetention>,
    deadline: Instant,
) {
    let plan = actordb_query::parse(&request.sql);
    let fragment = FragmentId(request.fragment);
    let page = match plan {
        Err(error) => page_error(&request, QueryPageStatus::Invalid, error.to_string()),
        Ok(plan) => {
            if !serving.read().unwrap().contains(&fragment) {
                page_error(
                    &request,
                    QueryPageStatus::Unavailable,
                    "fragment is not serving",
                )
            } else if let Some(actor) = router.route_for_fragment(fragment) {
                actor_page(
                    actor,
                    Arc::new(plan),
                    request.clone(),
                    retention.clone(),
                    deadline,
                )
                .await
                .unwrap_or_else(|error| {
                    page_error(&request, QueryPageStatus::Unavailable, error.to_string())
                })
            } else {
                page_error(&request, QueryPageStatus::Unavailable, "no fragment actor")
            }
        }
    };
    let _ = try_send_envelope_until(&channel, page_envelope(page), deadline).await;
}

pub(super) fn reserve_inbound(
    admission: &QueryAdmission,
    request: &QueryPageRequest,
) -> Result<Arc<QueryRetention>, QueryExecutionError> {
    if request.sql.len() > 1024 * 1024 {
        return Err(QueryExecutionError::BudgetExceeded(
            "SQL text exceeds 1 MiB".into(),
        ));
    }
    let page_bytes = if request.max_bytes == 0 {
        1024 * 1024
    } else {
        request.max_bytes as usize
    };
    if !(1024..=MAX_QUERY_PAGE_BYTES).contains(&page_bytes) {
        return Err(QueryExecutionError::InvalidQuery(
            "invalid page byte budget".into(),
        ));
    }
    admission.reserve_work(
        request
            .sql
            .capacity()
            .saturating_mul(4)
            .saturating_add(request.after_key.capacity())
            .saturating_add(1024),
        page_bytes.saturating_mul(32).saturating_add(4096),
    )
}

#[cfg(test)]
mod page_validation_tests {
    use super::*;
    use crate::query::value_to_cell;

    #[tokio::test]
    async fn optional_query_and_scan_capabilities_preserve_old_peer_connections() {
        let mut hello = super::super::fresh_hello("old-peer").unwrap();
        hello.capabilities = super::super::REQUIRED_WIRE_CAPABILITIES;
        super::super::validate_peer_hello("local", None, &hello).unwrap();
        let node = Node::start(
            "local".into(),
            "127.0.0.1:0".parse().unwrap(),
            super::super::NodeConfig {
                fragment_map: HashMap::from([(FragmentId(0), vec!["old-peer".into()])]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let (mut channel, mut requests, mut responses) = super::super::tests::test_peer_channel(4);
        channel.capabilities = super::super::REQUIRED_WIRE_CAPABILITIES;
        node.peers.lock().await.insert("old-peer".into(), channel);
        assert!(matches!(
            node.query("SELECT COUNT(*) FROM kv").await,
            Err(QueryExecutionError::FragmentsUnavailable(_))
        ));
        let spec = crate::actor::ScanSpec {
            matcher: crate::actor::KeyMatcher::All,
            transform: crate::actor::ValueTransform::KeyOnly,
            limit: 10,
            offset: 0,
        };
        assert!(node.scan(&spec).await.is_err());
        assert!(requests.try_recv().is_err());
        assert!(responses.try_recv().is_err());
        assert_eq!(node.peer_count().await, 1);
        node.shutdown().await.unwrap();
    }

    #[test]
    fn inbound_pages_share_runtime_admission_and_release_on_completion() {
        let admission = QueryAdmission::with_runtime_capacity(64 * 1024);
        let request = QueryPageRequest {
            max_bytes: 1024,
            sql: "SELECT key FROM kv".into(),
            ..Default::default()
        };
        let first = reserve_inbound(&admission, &request).unwrap();
        assert!(reserve_inbound(&admission, &request).is_err());
        assert_eq!(
            admission.diagnostics().reserved_runtime_bytes,
            32 * 1024 + 4096
        );
        drop(first);
        assert_eq!(admission.diagnostics().reserved_runtime_bytes, 0);
        assert!(reserve_inbound(&admission, &request).is_ok());
    }

    fn page() -> (QueryPageRequest, QueryPlan, QueryPageResponse) {
        let fragment = fragment_of("a", 4).0;
        let req = QueryPageRequest {
            req_id: 9,
            fragment,
            max_rows: 256,
            max_bytes: 1024 * 1024,
            max_visits: 256,
            ..Default::default()
        };
        let plan =
            actordb_query::parse("SELECT key, frag, PREFIX_BYTES(value, 2) FROM kv").unwrap();
        let page = QueryPageResponse {
            req_id: 9,
            fragment,
            rows: vec![crate::proto::QueryPageRow {
                key: "a".into(),
                cells: vec![
                    value_to_cell(QueryValue::Text("a".into())),
                    value_to_cell(QueryValue::Integer(fragment as i128)),
                    value_to_cell(QueryValue::Bytes(vec![1, 2])),
                ],
            }],
            next_after_key: "a".into(),
            done: true,
            revision: 3,
            snapshot_time_ms: 4,
            incarnation: 5,
            visited: 1,
            status: QueryPageStatus::Ok as i32,
            ..Default::default()
        };
        (req, plan, page)
    }

    #[test]
    fn rejects_wrong_projected_cells_and_unbounded_prefixes() {
        let (req, plan, original) = page();
        validate_page(&original, &req, &plan, 4).unwrap();
        for (index, value) in [
            (0, QueryValue::Text("other".into())),
            (1, QueryValue::Integer((req.fragment + 1) as i128)),
            (2, QueryValue::Bytes(vec![1, 2, 3])),
            (0, QueryValue::Bytes(vec![1])),
        ] {
            let mut forged = original.clone();
            forged.rows[0].cells[index] = value_to_cell(value);
            assert!(matches!(
                validate_page(&forged, &req, &plan, 4),
                Err(QueryExecutionError::Protocol(_))
            ));
        }
    }

    #[test]
    fn rejects_wrong_fragment_keys_and_impossible_aggregate_counts() {
        let (req, plan, mut forged) = page();
        let wrong_key = (0..100)
            .map(|n| format!("wrong{n}"))
            .find(|key| fragment_of(key, 4).0 != req.fragment)
            .unwrap();
        forged.rows[0].key = wrong_key.clone();
        forged.rows[0].cells[0] = value_to_cell(QueryValue::Text(wrong_key.clone()));
        forged.next_after_key = wrong_key;
        assert!(validate_page(&forged, &req, &plan, 4).is_err());
        let aggregate = actordb_query::parse("SELECT COUNT(*) FROM kv").unwrap();
        forged.rows = vec![crate::proto::QueryPageRow {
            key: String::new(),
            cells: vec![value_to_cell(QueryValue::Integer(2))],
        }];
        forged.visited = 1;
        assert!(validate_page(&forged, &req, &aggregate, 4).is_err());
    }

    #[test]
    fn rejects_completed_aggregate_continuation_with_regressed_cursor() {
        let (mut req, _, mut forged) = page();
        req.continuation = true;
        req.after_key = "b".into();
        req.revision = forged.revision;
        req.snapshot_time_ms = forged.snapshot_time_ms;
        req.incarnation = forged.incarnation;
        forged.rows = vec![crate::proto::QueryPageRow {
            key: String::new(),
            cells: vec![value_to_cell(QueryValue::Integer(1))],
        }];
        forged.done = true;
        let aggregate = actordb_query::parse("SELECT COUNT(*) FROM kv").unwrap();
        assert!(validate_page(&forged, &req, &aggregate, 4).is_err());
    }

    #[test]
    fn rejects_wrong_correlation_and_nonprogressing_continuations() {
        let (mut req, plan, original) = page();
        let mut forged = original.clone();
        forged.req_id += 1;
        assert!(validate_page(&forged, &req, &plan, 4).is_err());
        req.continuation = true;
        req.after_key = "a".into();
        req.revision = original.revision;
        req.snapshot_time_ms = original.snapshot_time_ms;
        req.incarnation = original.incarnation;
        let mut forged = original.clone();
        forged.rows.clear();
        forged.done = false;
        assert!(validate_page(&forged, &req, &plan, 4).is_err());
        forged.done = true;
        forged.incarnation += 1;
        assert!(validate_page(&forged, &req, &plan, 4).is_err());
    }
}
