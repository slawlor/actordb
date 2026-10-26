//! Server coordinator for bounded, revision-guarded query pages.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use actordb_query::{QueryAccumulator, QueryPlan, QueryResult, QueryValue};
use ractor::{rpc::CallResult, ActorRef};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{try_send_envelope_until, FragmentRouter, Node, OperationPermit, PeerChannel};
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
    row_storage_bytes: usize,
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
            row_storage_bytes: 0,
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
            .saturating_add(self.row_storage_bytes)
    }

    fn accept_page(
        &mut self,
        page: QueryPageResponse,
        buffer: &mut QueryBufferUsage,
    ) -> Result<(), QueryExecutionError> {
        let previous = self.storage();
        self.revision = page.revision;
        self.time = page.snapshot_time_ms;
        self.incarnation = page.incarnation;
        self.after_key = page.next_after_key;
        self.done = page.done;
        self.initialized = true;
        self.row_storage_bytes = page.rows.iter().try_fold(0usize, |bytes, row| {
            bytes
                .checked_add(row_storage(row))
                .ok_or_else(buffer_overflow)
        })?;
        self.rows = page.rows.into();
        buffer.replace(previous, self.storage())
    }

    fn pop_row(
        &mut self,
        buffer: &mut QueryBufferUsage,
    ) -> Result<crate::proto::QueryPageRow, QueryExecutionError> {
        let previous = self.storage();
        let row = self.rows.pop_front().expect("selected head exists");
        self.row_storage_bytes = self
            .row_storage_bytes
            .checked_sub(row_storage(&row))
            .ok_or_else(buffer_overflow)?;
        buffer.replace(previous, self.storage())?;
        Ok(row)
    }
}

// One initial O(fragments) sum, followed only by deltas for the cursor that
// changed. Rows are measured once on arrival and once on consumption; no merge
// step walks other cursors or already-buffered rows. Cursor storage still
// conservatively reserves deque backing headers and the heap's cloned head key.
struct QueryBufferUsage {
    bytes: usize,
}

impl QueryBufferUsage {
    fn new(cursors: &[FragmentCursor], sql_bytes: usize) -> Result<Self, QueryExecutionError> {
        let bytes = cursors.iter().try_fold(sql_bytes, |bytes, cursor| {
            bytes
                .checked_add(cursor.storage())
                .ok_or_else(buffer_overflow)
        })?;
        Ok(Self { bytes })
    }

    fn replace(&mut self, previous: usize, current: usize) -> Result<(), QueryExecutionError> {
        self.bytes = self
            .bytes
            .checked_sub(previous)
            .and_then(|bytes| bytes.checked_add(current))
            .ok_or_else(buffer_overflow)?;
        Ok(())
    }
}

fn buffer_overflow() -> QueryExecutionError {
    QueryExecutionError::BudgetExceeded("query buffer accounting overflow".into())
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
        plan.validate()?;
        let table = self
            .table(plan.table.clone())
            .map_err(|error| QueryExecutionError::InvalidQuery(error.to_string()))?;
        table.query_plan(plan).await
    }

    /// Execute a query under one absolute deadline and explicit memory/work limits.
    pub async fn query_plan_with_options(
        &self,
        plan: &QueryPlan,
        options: QueryOptions,
    ) -> Result<QueryResult, QueryExecutionError> {
        plan.validate()?;
        let table = self
            .table(plan.table.clone())
            .map_err(|error| QueryExecutionError::InvalidQuery(error.to_string()))?;
        table.query_plan_with_options(plan, options).await
    }

    pub(super) async fn query_plan_with_options_scoped(
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
        let operation = Arc::new(
            self.admit_table_operation(&plan.table)
                .map_err(|_| QueryExecutionError::ShuttingDown)?,
        );
        let syntax_limits = crate::query::validate_physical_plan(plan)?;
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
        if sql.len() > syntax_limits.max_canonical_sql_bytes() {
            return Err(QueryExecutionError::BudgetExceeded(
                "query text exceeds bounded physical-plan serialization".into(),
            ));
        }
        let retention = self.query_admission.reserve_work_retained(
            sql.len().saturating_mul(4).saturating_add(1024),
            options
                .max_result_bytes
                .checked_add(options.max_buffer_bytes)
                .ok_or_else(|| {
                    QueryExecutionError::BudgetExceeded("query runtime reservation overflow".into())
                })?,
            Some(operation.clone()),
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
        .map_err(|_| {
            self.note_waiter_timeout();
            QueryExecutionError::Timeout
        })?
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
        let mut buffer = QueryBufferUsage::new(cursors, sql.len())?;
        check_buffer(&buffer, options)?;
        let mut visited = 0u64;
        if plan.is_aggregate() {
            let mut aggregate = QueryAccumulator::new_with_limits(
                &plan,
                crate::query::validate_physical_plan(&plan)?,
            );
            // One active fragment page keeps aggregate memory independent of
            // record count; the accumulator merges exact partial states.
            for cursor in cursors.iter_mut() {
                while !cursor.done {
                    let page_options = options_for_page(cursor, &buffer, options)?;
                    self.fetch_query_page(
                        cursor,
                        &plan,
                        sql,
                        &page_options,
                        retention.clone(),
                        deadline,
                        false,
                        &mut visited,
                        &mut buffer,
                    )
                    .await?;
                    check_buffer(&buffer, options)?;
                    let previous = cursor.storage();
                    let partial = QueryResult {
                        columns: plan.columns(),
                        rows: cursor
                            .rows
                            .drain(..)
                            .map(|row| row.cells.into_iter().map(cell_to_value).collect())
                            .collect::<Result<_, _>>()?,
                    };
                    cursor.row_storage_bytes = 0;
                    buffer.replace(previous, cursor.storage())?;
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
            for (index, cursor) in cursors.iter_mut().enumerate() {
                let page_options = options_for_page(cursor, &buffer, options)?;
                self.fill_query_head(
                    cursor,
                    &plan,
                    sql,
                    &page_options,
                    retention.clone(),
                    deadline,
                    &mut visited,
                    &mut buffer,
                )
                .await?;
                check_buffer(&buffer, options)?;
                if let Some(row) = cursor.rows.front() {
                    heads.push(Reverse((row.key.clone(), index)));
                }
            }
            let mut skipped = 0u64;
            let mut payload_bytes = 0usize;
            while let Some(Reverse((key, index))) = heads.pop() {
                drop(key);
                let row = cursors[index].pop_row(&mut buffer)?;
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
                if cursors[index].rows.is_empty() && !cursors[index].done {
                    let page_options = options_for_page(&cursors[index], &buffer, options)?;
                    self.fill_query_head(
                        &mut cursors[index],
                        &plan,
                        sql,
                        &page_options,
                        retention.clone(),
                        deadline,
                        &mut visited,
                        &mut buffer,
                    )
                    .await?;
                    check_buffer(&buffer, options)?;
                }
                if let Some(row) = cursors[index].rows.front() {
                    heads.push(Reverse((row.key.clone(), index)));
                }
            }
        }
        // Recheck every accepted contribution, including completed fragments,
        // before releasing a successful result. This is per-fragment checking;
        // the check instants do not create a distributed atomic snapshot.
        for cursor in cursors.iter_mut() {
            if !cursor.initialized {
                continue;
            }
            let page_options = options_for_page(cursor, &buffer, options)?;
            self.fetch_query_page(
                cursor,
                &plan,
                sql,
                &page_options,
                retention.clone(),
                deadline,
                true,
                &mut visited,
                &mut buffer,
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
        buffer: &mut QueryBufferUsage,
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
                buffer,
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
        buffer: &mut QueryBufferUsage,
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
                    holders.push(Holder::Remote(channel));
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
        cursor.accept_page(page, buffer)
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
    cursor: &FragmentCursor,
    buffer: &QueryBufferUsage,
    options: &QueryOptions,
) -> Result<QueryOptions, QueryExecutionError> {
    let available = options.max_buffer_bytes.saturating_sub(buffer.bytes);
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
    page_options.max_buffer_bytes = available.saturating_add(cursor.storage());
    Ok(page_options)
}

fn check_buffer(
    buffer: &QueryBufferUsage,
    options: &QueryOptions,
) -> Result<(), QueryExecutionError> {
    if buffer.bytes > options.max_buffer_bytes {
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
    let validation_limits = crate::query::validate_physical_plan(&plan)?;
    let work = QueryPageWork {
        plan,
        request,
        retention: Some(retention),
        validation_limits,
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
    mut channel: PeerChannel,
    router: FragmentRouter,
    serving: Arc<std::sync::RwLock<std::collections::HashSet<FragmentId>>>,
    request: QueryPageRequest,
    retention: Arc<QueryRetention>,
    deadline: Instant,
    operation: OperationPermit,
) {
    // The exact request/peer leases follow the encoded response through queue
    // admission and actual writer completion, including error pages.
    channel.work_retention = Some(Arc::new((
        channel.work_retention.clone(),
        retention.clone(),
        operation.clone(),
    )));
    let plan = crate::query::parse_physical_plan(&request.sql);
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
    operation.mark_responding();
    let _ = try_send_envelope_until(&channel, page_envelope(page), deadline).await;
}

pub(super) fn reserve_inbound(
    admission: &QueryAdmission,
    request: &QueryPageRequest,
    operation: Arc<dyn Send + Sync>,
) -> Result<Arc<QueryRetention>, QueryExecutionError> {
    let limits = actordb_query::QueryLimits::for_namespace_encoding(1024)?;
    if request.sql.len() > limits.max_sql_bytes() {
        return Err(QueryExecutionError::BudgetExceeded(
            "SQL text exceeds bounded physical-plan serialization".into(),
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
    admission.reserve_work_retained(
        request
            .sql
            .capacity()
            .saturating_mul(4)
            .saturating_add(request.after_key.capacity())
            .saturating_add(1024),
        page_bytes.saturating_mul(32).saturating_add(4096),
        Some(operation),
    )
}

#[cfg(test)]
mod page_validation_tests {
    use super::*;
    use crate::query::value_to_cell;

    fn reference_storage(cursor: &FragmentCursor) -> usize {
        std::mem::size_of::<FragmentCursor>()
            + cursor.after_key.capacity()
            + cursor.rows.front().map_or(0, |row| row.key.capacity())
            + cursor.rows.capacity() * std::mem::size_of::<crate::proto::QueryPageRow>()
            + cursor.rows.iter().map(row_storage).sum::<usize>()
    }

    fn assert_reference(cursors: &[FragmentCursor], sql_bytes: usize, buffer: &QueryBufferUsage) {
        for cursor in cursors {
            assert_eq!(cursor.storage(), reference_storage(cursor));
            assert_eq!(
                cursor.row_storage_bytes,
                cursor.rows.iter().map(row_storage).sum::<usize>()
            );
        }
        assert_eq!(
            buffer.bytes,
            sql_bytes + cursors.iter().map(reference_storage).sum::<usize>()
        );
    }

    fn accounting_page(seed: usize, count: usize) -> QueryPageResponse {
        let rows = (0..count)
            .map(|index| {
                let mut key = String::with_capacity(40 + seed + index);
                key.push_str(&format!("key:{seed}:{index}"));
                let mut text = String::with_capacity(60 + seed);
                text.push_str("projected text");
                let mut bytes = Vec::with_capacity(80 + seed);
                bytes.extend_from_slice(&[1, 2, 3]);
                crate::proto::QueryPageRow {
                    key,
                    cells: vec![
                        value_to_cell(QueryValue::Text(text)),
                        value_to_cell(QueryValue::Bytes(bytes)),
                        value_to_cell(QueryValue::Integer(index as i128)),
                        value_to_cell(QueryValue::Null),
                    ],
                }
            })
            .collect();
        let mut next_after_key = String::with_capacity(100 + seed);
        next_after_key.push_str(&format!("continuation:{seed}"));
        QueryPageResponse {
            rows,
            next_after_key,
            revision: 7,
            incarnation: 8,
            snapshot_time_ms: 9,
            ..Default::default()
        }
    }

    #[test]
    fn incremental_buffer_matches_reference_across_pages_rows_and_sparse_continuations() {
        let sql_bytes = 97;
        let mut cursors: Vec<_> = (0..64).map(FragmentCursor::new).collect();
        let mut buffer = QueryBufferUsage::new(&cursors, sql_bytes).unwrap();
        assert_reference(&cursors, sql_bytes, &buffer);
        for index in 0..cursors.len() {
            cursors[index]
                .accept_page(accounting_page(index, index % 5 + 1), &mut buffer)
                .unwrap();
            assert_reference(&cursors, sql_bytes, &buffer);
        }
        // Interleaved consumption changes head-key reservation but retains the
        // deque backing capacity, even after its final row is consumed.
        for _ in 0..5 {
            for index in 0..cursors.len() {
                if !cursors[index].rows.is_empty() {
                    cursors[index].pop_row(&mut buffer).unwrap();
                    assert_reference(&cursors, sql_bytes, &buffer);
                }
            }
        }
        for index in 0..cursors.len() {
            cursors[index]
                .accept_page(accounting_page(index + 300, 0), &mut buffer)
                .unwrap();
            assert_reference(&cursors, sql_bytes, &buffer);
        }
        cursors[0]
            .accept_page(accounting_page(1, 256), &mut buffer)
            .unwrap();
        let previous = cursors[0].storage();
        let partial: Vec<_> = cursors[0].rows.drain(..).collect();
        cursors[0].row_storage_bytes = 0;
        buffer.replace(previous, cursors[0].storage()).unwrap();
        assert_eq!(partial.len(), 256);
        assert_reference(&cursors, sql_bytes, &buffer);
    }

    #[test]
    fn incremental_buffer_preserves_page_allowance_and_rejects_overflow() {
        let mut cursors = vec![FragmentCursor::new(0), FragmentCursor::new(1)];
        let mut buffer = QueryBufferUsage::new(&cursors, 111).unwrap();
        cursors[0]
            .accept_page(accounting_page(30, 32), &mut buffer)
            .unwrap();
        let options = QueryOptions {
            max_buffer_bytes: 256 * 1024,
            ..Default::default()
        };
        let page_options = options_for_page(&cursors[1], &buffer, &options).unwrap();
        let available =
            options.max_buffer_bytes - 111 - cursors.iter().map(reference_storage).sum::<usize>();
        assert_eq!(
            page_options.page_bytes,
            options.page_bytes.min(available.saturating_sub(4096) / 32)
        );
        assert_eq!(
            page_options.max_buffer_bytes,
            available + reference_storage(&cursors[1])
        );
        let mut exact = options.clone();
        exact.max_buffer_bytes = buffer.bytes;
        assert!(check_buffer(&buffer, &exact).is_ok());
        exact.max_buffer_bytes -= 1;
        assert!(check_buffer(&buffer, &exact).is_err());
        assert!(options_for_page(&cursors[1], &buffer, &exact).is_err());
        let mut overflow = QueryBufferUsage { bytes: usize::MAX };
        assert!(overflow.replace(0, 1).is_err());
        let mut underflow = QueryBufferUsage { bytes: 0 };
        assert!(underflow.replace(1, 0).is_err());
    }

    #[test]
    fn inbound_pages_share_runtime_admission_and_release_on_completion() {
        let admission = QueryAdmission::with_runtime_capacity(64 * 1024);
        let request = QueryPageRequest {
            max_bytes: 1024,
            sql: "SELECT key FROM kv".into(),
            ..Default::default()
        };
        let first = reserve_inbound(&admission, &request, Arc::new(())).unwrap();
        assert!(reserve_inbound(&admission, &request, Arc::new(())).is_err());
        assert_eq!(
            admission.diagnostics().reserved_runtime_bytes,
            32 * 1024 + 4096
        );
        drop(first);
        assert_eq!(admission.diagnostics().reserved_runtime_bytes, 0);
        assert!(reserve_inbound(&admission, &request, Arc::new(())).is_ok());
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
