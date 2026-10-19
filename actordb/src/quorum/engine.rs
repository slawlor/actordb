//! Per-fragment Paxos over complete, bounded snapshots.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::future::join_all;
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::{watch, Mutex, Notify, RwLock, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use super::{
    state, transport, wire, ChangePage, CommitReceipt, FragmentSnapshot, Manifest, Membership,
    Mutation, MutationResult, QuorumConfig, QuorumError, Record,
};
use crate::actor::now_ms;
use crate::placement::validate_placement;
use crate::shard::{fragment_of, FragmentId};
use crate::store::StoreBinding;

struct Fragment {
    replica: Mutex<wire::ReplicaState>,
    proposer: Mutex<()>,
}

pub(super) struct Core {
    pub(super) id: String,
    pub(super) config: QuorumConfig,
    pub(super) policy: Vec<u8>,
    fragments: Vec<Fragment>,
    failed: AtomicBool,
    closed: AtomicBool,
    operations: RwLock<()>,
    admission: Semaphore,
    changes: Notify,
    binding: Mutex<Option<StoreBinding>>,
}

struct Lifecycle {
    close: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    shutdown: Mutex<()>,
}

impl Drop for Lifecycle {
    fn drop(&mut self) {
        let _ = self.close.send(true);
    }
}

/// A separately configured quorum endpoint. Clones share one runtime; shutting
/// down any clone closes admission for all clones and joins accepted work.
#[derive(Clone)]
pub struct QuorumNode {
    core: Arc<Core>,
    lifecycle: Arc<Lifecycle>,
    address: SocketAddr,
}

fn validate_config(id: &str, config: &QuorumConfig) -> Result<(), QuorumError> {
    let invalid = |reason: &str| QuorumError::Invalid(reason.into());
    if !state::valid_id(id)
        || !state::valid_id(&config.cluster_id)
        || !config.members.contains_key(id)
        || config.fragment_map.is_empty()
        || config.fragment_map.len() > u32::MAX as usize
    {
        return Err(invalid("identity, address book, or fragment count"));
    }
    validate_placement(&config.fragment_map, config.fragment_map.len())
        .map_err(QuorumError::Invalid)?;
    for voters in config.fragment_map.values() {
        state::validate_membership(&Membership {
            voters: voters.clone(),
            next_voters: vec![],
        })?;
        if voters.iter().any(|id| !config.members.contains_key(id)) {
            return Err(invalid("voter missing from address book"));
        }
        let membership = Membership {
            voters: voters.clone(),
            next_voters: vec![],
        };
        let genesis = state::genesis(membership, FragmentId(0), &config.cluster_id);
        if state::snapshot(state::committed(&genesis)).encoded_len()
            > (config.max_state_bytes / 3).saturating_sub(64)
        {
            return Err(invalid("genesis membership exceeds snapshot budget"));
        }
    }
    let mut addresses = std::collections::HashSet::new();
    if config
        .members
        .iter()
        .any(|(id, address)| !state::valid_id(id) || !addresses.insert(address))
    {
        return Err(invalid("duplicate address or invalid member ID"));
    }
    let now = Instant::now();
    for duration in [
        config.operation_timeout,
        config.rpc_timeout,
        config.maintenance_interval,
        config.max_request_lifetime,
        config.change_retention,
    ] {
        if duration.is_zero()
            || duration.as_millis() > u64::MAX as u128
            || now.checked_add(duration).is_none()
        {
            return Err(invalid("invalid quorum duration"));
        }
    }
    if config
        .operation_timeout
        .checked_add(config.rpc_timeout)
        .and_then(|d| now.checked_add(d))
        .is_none()
        || config.cluster_secret.as_ref().is_some_and(|s| s.len() < 32)
        || !(4096..=(16 * 1024 * 1024 - 8192) / 2).contains(&config.max_state_bytes)
        || !(4..=4096).contains(&config.max_inflight)
        || [config.max_records, config.max_receipts, config.max_changes]
            .iter()
            .any(|n| !(1..=100_000).contains(n))
    {
        return Err(invalid("invalid quorum credentials or resource limits"));
    }
    Ok(())
}

fn manifest(id: &str, config: &QuorumConfig) -> Manifest {
    let genesis: Vec<_> = (0..config.fragment_map.len())
        .map(|f| Membership {
            voters: config.fragment_map[&FragmentId(f as u32)].clone(),
            next_voters: vec![],
        })
        .collect();
    let mut digest = Sha256::new();
    for m in &genesis {
        let bytes = m.encode_to_vec();
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    for number in [
        config.max_records as u64,
        config.max_receipts as u64,
        config.max_changes as u64,
        config.max_state_bytes as u64,
        config.max_request_lifetime.as_millis() as u64,
        config.change_retention.as_millis() as u64,
    ] {
        digest.update(number.to_be_bytes());
    }
    Manifest {
        version: 1,
        cluster_id: config.cluster_id.clone(),
        node_id: id.into(),
        genesis,
        policy: digest.finalize().to_vec(),
    }
}

impl QuorumNode {
    /// Bind stable identity before listening. A missing/corrupt fragment file
    /// fails startup; it must never be interpreted as a fresh voting replica.
    pub async fn start(
        id: String,
        address: SocketAddr,
        config: QuorumConfig,
    ) -> Result<Self, QuorumError> {
        validate_config(&id, &config)?;
        let manifest = manifest(&id, &config);
        let binding = config.store.bind(&manifest)?;
        let mut fragments = Vec::new();
        for index in 0..config.fragment_map.len() {
            let fragment = FragmentId(index as u32);
            let bytes = config
                .store
                .load(fragment)?
                .ok_or_else(|| QuorumError::Storage("missing initialized fragment state".into()))?;
            if bytes.len() > config.max_state_bytes {
                return Err(QuorumError::Storage(
                    "persisted quorum state exceeds policy".into(),
                ));
            }
            transport::preflight_state(&bytes, &config)
                .map_err(|e| QuorumError::Storage(e.to_string()))?;
            let replica = wire::ReplicaState::decode(bytes.as_slice())
                .map_err(|e| QuorumError::Storage(e.to_string()))?;
            state::validate_state(&replica, fragment, &config)
                .map_err(|e| QuorumError::Storage(e.to_string()))?;
            fragments.push(Fragment {
                replica: Mutex::new(replica),
                proposer: Mutex::new(()),
            });
        }
        let (address, mut acceptor) = config
            .network
            .bind(address)
            .await
            .map_err(|e| QuorumError::Invalid(e.to_string()))?;
        let admission = Semaphore::new(config.max_inflight / 4);
        let core = Arc::new(Core {
            id,
            policy: manifest.policy,
            config,
            fragments,
            failed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            operations: RwLock::new(()),
            admission,
            changes: Notify::new(),
            binding: Mutex::new(Some(binding)),
        });
        let (close, mut closing) = watch::channel(false);
        let serving = core.clone();
        let task = tokio::spawn(async move {
            let connections = Arc::new(Semaphore::new(serving.config.max_inflight));
            let mut tasks = JoinSet::new();
            let mut ticker = tokio::time::interval(serving.config.maintenance_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut maintenance_running = false;
            loop {
                tokio::select! {
                    biased;
                    _ = closing.changed() => break,
                    Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                        if matches!(result, Ok(true)) { maintenance_running = false; }
                    }
                    _ = ticker.tick(), if !maintenance_running => {
                        maintenance_running = true;
                        let core = serving.clone();
                        tasks.spawn(async move {
                            core.maintenance().await;
                            true
                        });
                    }
                    incoming = acceptor.accept() => {
                        match incoming {
                            Ok(stream) => {
                                if let Ok(permit) = connections.clone().try_acquire_owned() {
                                    let core = serving.clone();
                                    tasks.spawn(async move {
                                        let _permit = permit;
                                        transport::serve(core, stream).await;
                                        false
                                    });
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, "quorum listener failed");
                                break;
                            }
                        }
                    }
                }
            }
            drop(acceptor);
            while tasks.join_next().await.is_some() {}
        });
        Ok(Self {
            core,
            address,
            lifecycle: Arc::new(Lifecycle {
                close,
                task: Mutex::new(Some(task)),
                shutdown: Mutex::new(()),
            }),
        })
    }

    pub fn id(&self) -> &str {
        &self.core.id
    }
    pub fn addr(&self) -> SocketAddr {
        self.address
    }

    /// Route to a preferred writer, with quorum-fenced takeover on failure.
    /// A successful reply always carries a stable majority commit receipt.
    pub async fn mutate(&self, mutation: Mutation) -> Result<MutationResult, QuorumError> {
        let fragment = fragment_of(&mutation.key, self.core.fragments.len());
        let response = self
            .core
            .route(wire::Command {
                fragment: fragment.0,
                operation: Some(wire::command::Operation::Mutation(mutation.into_wire())),
            })
            .await?;
        let result = response
            .mutation
            .ok_or_else(|| QuorumError::Invalid("missing mutation result".into()))?;
        Ok(MutationResult {
            applied: result.applied,
            current: result.current,
            revision: result.revision,
            commit: receipt(response.commit)?,
        })
    }

    /// A linearizable read. It recovers any accepted predecessor and commits a
    /// read barrier; it never serves an unverified replica cache.
    pub async fn get(&self, key: String) -> Result<Option<Record>, QuorumError> {
        let fragment = fragment_of(&key, self.core.fragments.len());
        let response = self
            .core
            .route(wire::Command {
                fragment: fragment.0,
                operation: Some(wire::command::Operation::Get(key)),
            })
            .await?;
        Ok(response.record)
    }

    pub async fn changes(
        &self,
        fragment: FragmentId,
        after: u64,
        limit: usize,
    ) -> Result<ChangePage, QuorumError> {
        if limit == 0 || limit > self.core.config.max_changes {
            return Err(QuorumError::Invalid("invalid change page limit".into()));
        }
        let response = self
            .core
            .route(wire::Command {
                fragment: fragment.0,
                operation: Some(wire::command::Operation::Changes(wire::ChangesRequest {
                    after,
                    limit: limit as u32,
                })),
            })
            .await?;
        let cursor = response.changes.last().map_or(after, |c| c.id);
        Ok(ChangePage {
            changes: response.changes,
            cursor,
            last_change_id: response.last_change_id,
            commit: receipt(response.commit)?,
        })
    }

    /// Replayable notifications. The caller persists its cursor after handling
    /// each page; cancellation never advances it. Expiry is driven separately.
    pub fn subscribe(&self, fragment: FragmentId, after: u64) -> ChangeSubscription {
        ChangeSubscription {
            node: self.clone(),
            fragment,
            cursor: after,
        }
    }

    /// Recover a consistent full view after a retention gap. Resume changes
    /// after `last_change_id` to avoid a snapshot/subscription race.
    pub async fn snapshot(&self, fragment: FragmentId) -> Result<FragmentSnapshot, QuorumError> {
        let response = self
            .core
            .route(wire::Command {
                fragment: fragment.0,
                operation: Some(wire::command::Operation::Snapshot(true)),
            })
            .await?;
        let certificate = response
            .commit
            .ok_or_else(|| QuorumError::Invalid("missing snapshot certificate".into()))?;
        let snapshot = state::snapshot(&certificate);
        let records = snapshot
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.key.clone(),
                    entry.record.as_ref().expect("validated record").clone(),
                )
            })
            .collect();
        let last_change_id = snapshot.last_change_id;
        let time_ms = snapshot.time_ms;
        Ok(FragmentSnapshot {
            records,
            last_change_id,
            time_ms,
            commit: receipt(Some(certificate))?,
        })
    }

    /// Move through old -> joint(old,new) -> new. A timeout can leave the joint
    /// configuration active; retrying the same target safely completes it.
    /// Prospective members must be in every node's configured address book.
    pub async fn reconfigure(
        &self,
        fragment: FragmentId,
        voters: Vec<String>,
    ) -> Result<CommitReceipt, QuorumError> {
        for finish in [false, true] {
            let response = self
                .core
                .route(wire::Command {
                    fragment: fragment.0,
                    operation: Some(wire::command::Operation::Reconfigure(wire::Reconfigure {
                        voters: voters.clone(),
                        finish,
                    })),
                })
                .await?;
            if finish {
                return receipt(response.commit);
            }
        }
        unreachable!("two transition steps")
    }

    /// Stop admission, join accepted protocol work, then release storage.
    /// Cancellation leaves the retained join handle available for retry.
    pub async fn shutdown(&self) -> Result<(), QuorumError> {
        let _shutdown = self.lifecycle.shutdown.lock().await;
        self.core.closed.store(true, Ordering::Release);
        self.core.changes.notify_waiters();
        let _ = self.lifecycle.close.send(true);
        let mut task = self.lifecycle.task.lock().await;
        if let Some(handle) = task.as_mut() {
            handle
                .await
                .map_err(|e| QuorumError::Storage(format!("quorum runtime failed: {e}")))?;
            task.take();
        }
        let _operations = self.core.operations.write().await;
        self.core.binding.lock().await.take();
        Ok(())
    }
}

fn receipt(certificate: Option<wire::Certificate>) -> Result<CommitReceipt, QuorumError> {
    let certificate =
        certificate.ok_or_else(|| QuorumError::Invalid("missing commit receipt".into()))?;
    Ok(CommitReceipt {
        revision: certificate
            .snapshot
            .ok_or_else(|| QuorumError::Invalid("missing committed snapshot".into()))?
            .slot,
        term: certificate
            .ballot
            .ok_or_else(|| QuorumError::Invalid("missing committed term".into()))?,
        acknowledgements: certificate.acknowledgements,
        voting_membership: certificate
            .voting_membership
            .ok_or_else(|| QuorumError::Invalid("missing committed voters".into()))?,
    })
}

pub struct ChangeSubscription {
    node: QuorumNode,
    fragment: FragmentId,
    cursor: u64,
}

impl ChangeSubscription {
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    pub async fn next(&mut self) -> Result<ChangePage, QuorumError> {
        loop {
            let notified = self.node.core.changes.notified();
            let page = self
                .node
                .changes(
                    self.fragment,
                    self.cursor,
                    self.node.core.config.max_changes.min(256),
                )
                .await?;
            if !page.changes.is_empty() {
                self.cursor = page.cursor;
                return Ok(page);
            }
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep(self.node.core.config.maintenance_interval) => {},
            }
        }
    }
}

impl Core {
    pub(super) fn frame_limit(&self) -> usize {
        self.config.max_state_bytes * 2 + 4096
    }

    fn request(&self, fragment: FragmentId, operation: wire::request::Operation) -> wire::Request {
        wire::Request {
            cluster_id: self.config.cluster_id.clone(),
            node_id: self.id.clone(),
            fragment: fragment.0,
            operation: Some(operation),
        }
    }

    fn fragment(&self, fragment: FragmentId) -> Result<&Fragment, QuorumError> {
        self.fragments
            .get(fragment.0 as usize)
            .ok_or_else(|| QuorumError::Invalid("fragment out of range".into()))
    }

    fn save(&self, fragment: FragmentId, state: &wire::ReplicaState) -> Result<(), QuorumError> {
        state::validate_state(state, fragment, &self.config)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(QuorumError::Storage("node requires reopening".into()));
        }
        if let Err(error) = self.config.store.save(fragment, &state.encode_to_vec()) {
            self.failed.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    async fn route(&self, command: wire::Command) -> Result<wire::Response, QuorumError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(QuorumError::ShuttingDown);
        }
        let _operation = self.operations.read().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(QuorumError::ShuttingDown);
        }
        let _admission = self
            .admission
            .try_acquire()
            .map_err(|_| QuorumError::ResourceLimit("operation admission".into()))?;
        let fragment = FragmentId(command.fragment);
        let local = self.fragment(fragment)?.replica.lock().await;
        let mut targets = state::voters(state::membership(state::committed(&local)));
        drop(local);
        let mut extras: Vec<_> = self.config.members.keys().cloned().collect();
        extras.sort();
        targets.extend(
            extras
                .into_iter()
                .filter(|id| !targets_contains(&targets, id))
                .collect::<Vec<_>>(),
        );
        let deadline = Instant::now() + self.config.operation_timeout;
        let mut uncertain = false;
        for target in targets {
            if Instant::now() >= deadline {
                break;
            }
            let result = if target == self.id {
                self.propose(command.clone(), deadline).await
            } else {
                let request =
                    self.request(fragment, wire::request::Operation::Execute(command.clone()));
                match tokio::time::timeout_at(
                    deadline,
                    transport::call(self, &target, request, true),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(QuorumError::UnknownOutcome),
                }
            };
            match result {
                Ok(response) => match state::check_response(&response) {
                    Ok(()) => {
                        let commit = response.commit.as_ref().ok_or_else(|| {
                            QuorumError::Invalid("missing commit certificate".into())
                        })?;
                        state::validate_certificate(commit, fragment, &self.config)?;
                        self.learn(fragment, commit.clone()).await?;
                        return Ok(response);
                    }
                    Err(QuorumError::Unavailable | QuorumError::UnknownOutcome) => {
                        uncertain = true;
                    }
                    Err(QuorumError::Storage(_)) if target != self.id => {
                        uncertain = true;
                    }
                    Err(error) => return Err(error),
                },
                Err(
                    QuorumError::Invalid(_)
                    | QuorumError::Unavailable
                    | QuorumError::UnknownOutcome,
                ) => {
                    uncertain = true;
                }
                Err(error) => return Err(error),
            }
        }
        Err(if uncertain {
            QuorumError::UnknownOutcome
        } else {
            QuorumError::Unavailable
        })
    }

    pub(super) async fn handle(&self, request: wire::Request) -> wire::Response {
        if request.cluster_id != self.config.cluster_id
            || !self.config.members.contains_key(&request.node_id)
        {
            return state::error_response(QuorumError::Invalid("request origin".into()));
        }
        if let Some(wire::request::Operation::Execute(command)) = request.operation.as_ref() {
            if command.fragment != request.fragment {
                return state::error_response(QuorumError::Invalid(
                    "command fragment mismatch".into(),
                ));
            }
            let _operation = self.operations.read().await;
            if self.closed.load(Ordering::Acquire) {
                return state::error_response(QuorumError::ShuttingDown);
            }
            let Ok(_admission) = self.admission.try_acquire() else {
                return state::error_response(QuorumError::ResourceLimit(
                    "operation admission".into(),
                ));
            };
            return match self
                .propose(
                    command.clone(),
                    Instant::now() + self.config.operation_timeout,
                )
                .await
            {
                Ok(response) => response,
                Err(error) => state::error_response(error),
            };
        }
        match self.protocol(request).await {
            Ok(response) => response,
            Err(error) => state::error_response(error),
        }
    }

    async fn learn(
        &self,
        fragment: FragmentId,
        certificate: wire::Certificate,
    ) -> Result<(), QuorumError> {
        state::validate_certificate(&certificate, fragment, &self.config)?;
        let mut local = self.fragment(fragment)?.replica.lock().await;
        let slot = state::snapshot(&certificate).slot;
        let current = state::snapshot(state::committed(&local)).slot;
        if slot < current {
            return Ok(());
        }
        if slot == current {
            if certificate.snapshot != state::committed(&local).snapshot {
                return Err(QuorumError::Invalid("conflicting chosen snapshot".into()));
            }
            return Ok(());
        }
        let mut replacement = local.clone();
        replacement.committed = Some(certificate.clone());
        replacement.accepted = None;
        if let Some(ballot) = certificate.ballot {
            if replacement
                .promise
                .as_ref()
                .is_none_or(|p| state::ballot_cmp(p, &ballot).is_lt())
            {
                replacement.promise = Some(ballot);
            }
        }
        self.save(fragment, &replacement)?;
        *local = replacement;
        self.changes.notify_waiters();
        Ok(())
    }

    async fn protocol(&self, request: wire::Request) -> Result<wire::Response, QuorumError> {
        use wire::request::Operation;
        if self.failed.load(Ordering::Acquire) {
            return Err(QuorumError::Storage("node requires reopening".into()));
        }
        let fragment = FragmentId(request.fragment);
        self.fragment(fragment)?;
        let (base, ballot, proposal) = match request.operation {
            Some(Operation::Learn(certificate)) => {
                self.learn(fragment, certificate).await?;
                return Ok(wire::Response::default());
            }
            Some(Operation::Prepare(prepare)) => (prepare.base, prepare.ballot, None),
            Some(Operation::Accept(accept)) => {
                let ballot = accept.proposal.as_ref().and_then(|p| p.ballot.clone());
                (accept.base, ballot, accept.proposal)
            }
            _ => return Err(QuorumError::Invalid("expected consensus operation".into())),
        };
        let base = base.ok_or_else(|| QuorumError::Invalid("missing consensus base".into()))?;
        let ballot =
            ballot.ok_or_else(|| QuorumError::Invalid("missing consensus ballot".into()))?;
        state::validate_certificate(&base, fragment, &self.config)?;
        let voters = state::voters(state::membership(&base));
        if ballot.round == 0
            || ballot.node_id != request.node_id
            || !voters.contains(&ballot.node_id)
            || !voters.contains(&self.id)
        {
            return Err(QuorumError::Unavailable);
        }
        self.learn(fragment, base.clone()).await?;
        let mut local = self.fragment(fragment)?.replica.lock().await;
        let current = state::committed(&local);
        if state::snapshot(current).slot > state::snapshot(&base).slot {
            return Ok(wire::Response {
                status: wire::Status::Ahead as i32,
                state: Some(local.clone()),
                ..Default::default()
            });
        }
        if local
            .promise
            .as_ref()
            .is_some_and(|p| state::ballot_cmp(p, &ballot).is_gt())
        {
            return Ok(wire::Response {
                status: wire::Status::Rejected as i32,
                state: Some(local.clone()),
                ..Default::default()
            });
        }
        let mut replacement = local.clone();
        replacement.promise = Some(ballot.clone());
        if let Some(proposal) = proposal {
            let proposed = proposal
                .snapshot
                .as_ref()
                .ok_or_else(|| QuorumError::Invalid("missing proposal snapshot".into()))?;
            validate_successor(state::snapshot(&base), proposed, fragment, &self.config)?;
            if replacement.accepted.as_ref().is_some_and(|p| {
                p.ballot.as_ref() == Some(&ballot) && p.snapshot != proposal.snapshot
            }) {
                return Err(QuorumError::Invalid(
                    "ballot reused with another value".into(),
                ));
            }
            replacement.accepted = Some(proposal);
        }
        self.save(fragment, &replacement)?;
        *local = replacement;
        Ok(wire::Response {
            state: Some(local.clone()),
            ..Default::default()
        })
    }

    async fn phase(
        &self,
        fragment: FragmentId,
        members: &Membership,
        operation: wire::request::Operation,
    ) -> Vec<(String, wire::Response)> {
        join_all(state::voters(members).into_iter().map(|peer| {
            let request = self.request(fragment, operation.clone());
            async move {
                let response = if peer == self.id {
                    self.protocol(request).await
                } else {
                    transport::call(self, &peer, request, false).await
                };
                response.ok().map(|response| (peer, response))
            }
        }))
        .await
        .into_iter()
        .flatten()
        .collect()
    }

    async fn propose(
        &self,
        command: wire::Command,
        deadline: Instant,
    ) -> Result<wire::Response, QuorumError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(QuorumError::Storage("node requires reopening".into()));
        }
        let fragment = FragmentId(command.fragment);
        let entry = self.fragment(fragment)?;
        let _proposer = tokio::time::timeout_at(deadline, entry.proposer.lock())
            .await
            .map_err(|_| QuorumError::Unavailable)?;
        let mut command_id = vec![0; 16];
        getrandom::fill(&mut command_id).map_err(|e| QuorumError::Invalid(e.to_string()))?;
        let mut highest_round = 0;
        let mut attempted_accept = false;
        loop {
            if Instant::now() >= deadline {
                return Err(if attempted_accept {
                    QuorumError::UnknownOutcome
                } else {
                    QuorumError::Unavailable
                });
            }
            let local = entry.replica.lock().await.clone();
            let base = state::committed(&local).clone();
            let members = state::membership(&base).clone();
            if !state::voters(&members).contains(&self.id) {
                return Err(QuorumError::Unavailable);
            }
            highest_round = highest_round.max(local.promise.as_ref().map_or(0, |p| p.round));
            let ballot = wire::Ballot {
                round: highest_round
                    .checked_add(1)
                    .ok_or_else(|| QuorumError::ResourceLimit("ballot exhausted".into()))?,
                node_id: self.id.clone(),
            };
            highest_round = ballot.round;
            let prepared = self
                .phase(
                    fragment,
                    &members,
                    wire::request::Operation::Prepare(wire::Prepare {
                        base: Some(base.clone()),
                        ballot: Some(ballot.clone()),
                    }),
                )
                .await;
            let mut promises = Vec::new();
            let mut accepted: Option<wire::Proposal> = None;
            let mut ahead: Option<wire::Certificate> = None;
            for (peer, response) in prepared {
                let Some(remote) = response.state else {
                    continue;
                };
                state::validate_state(&remote, fragment, &self.config)?;
                highest_round = highest_round.max(remote.promise.as_ref().map_or(0, |p| p.round));
                let remote_base = state::committed(&remote);
                if state::snapshot(remote_base).slot > state::snapshot(&base).slot {
                    if ahead
                        .as_ref()
                        .is_none_or(|c| state::snapshot(c).slot < state::snapshot(remote_base).slot)
                    {
                        ahead = Some(remote_base.clone());
                    }
                    continue;
                }
                if response.status == wire::Status::Ok as i32
                    && remote.promise.as_ref() == Some(&ballot)
                {
                    promises.push(peer);
                    if let Some(candidate) = remote.accepted {
                        if accepted.as_ref().is_none_or(|p| {
                            state::ballot_cmp(
                                candidate.ballot.as_ref().expect("validated ballot"),
                                p.ballot.as_ref().expect("validated ballot"),
                            )
                            .is_gt()
                        }) {
                            accepted = Some(candidate);
                        }
                    }
                }
            }
            if let Some(ahead) = ahead {
                self.learn(fragment, ahead).await?;
                continue;
            }
            if !state::has_quorum(&members, &promises) {
                if highest_round > ballot.round {
                    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
                    continue;
                }
                return Err(if attempted_accept {
                    QuorumError::UnknownOutcome
                } else {
                    QuorumError::Unavailable
                });
            }
            let candidate = if let Some(accepted) = accepted {
                accepted.snapshot.expect("validated accepted snapshot")
            } else {
                make_candidate(state::snapshot(&base), &command, &command_id, &self.config)?
            };
            let proposal = wire::Proposal {
                ballot: Some(ballot.clone()),
                snapshot: Some(candidate.clone()),
            };
            let preflight = wire::ReplicaState {
                committed: Some(base.clone()),
                promise: Some(ballot.clone()),
                accepted: Some(proposal.clone()),
            };
            state::validate_state(&preflight, fragment, &self.config)?;
            attempted_accept = true;
            let accepted = self
                .phase(
                    fragment,
                    &members,
                    wire::request::Operation::Accept(wire::Accept {
                        base: Some(base.clone()),
                        proposal: Some(proposal),
                    }),
                )
                .await;
            let mut acknowledgements = Vec::new();
            for (peer, response) in accepted {
                if response.status == wire::Status::Ok as i32 {
                    if let Some(remote) = response.state {
                        state::validate_state(&remote, fragment, &self.config)?;
                        if remote.accepted.as_ref().is_some_and(|p| {
                            p.ballot.as_ref() == Some(&ballot)
                                && p.snapshot.as_ref() == Some(&candidate)
                        }) {
                            acknowledgements.push(peer);
                        }
                    }
                }
            }
            if !state::has_quorum(&members, &acknowledgements) {
                continue;
            }
            let certificate = wire::Certificate {
                snapshot: Some(candidate.clone()),
                ballot: Some(ballot),
                voting_membership: Some(members.clone()),
                acknowledgements,
            };
            self.learn(fragment, certificate.clone()).await?;
            // Include newly added and removed replicas. A disconnected replica
            // catches up through certified base snapshots before it can vote.
            let mut recipients = state::voters(&members);
            for id in state::voters(state::membership(&certificate)) {
                if !recipients.contains(&id) {
                    recipients.push(id);
                }
            }
            let learned = join_all(recipients.into_iter().filter(|id| id != &self.id).map(
                |peer| {
                    let request = self.request(
                        fragment,
                        wire::request::Operation::Learn(certificate.clone()),
                    );
                    async move {
                        transport::call(self, &peer, request, false)
                            .await
                            .ok()
                            .filter(|response| response.status == wire::Status::Ok as i32)
                            .map(|_| peer)
                    }
                },
            ))
            .await;
            if state::membership(&certificate) != &members {
                // A completed membership change must be recoverable without
                // the removed voters. Persist the decision certificate on a
                // quorum of the resulting configuration before acknowledging.
                let next = state::membership(&certificate);
                let next_voters = state::voters(next);
                let mut installed: Vec<_> = learned
                    .into_iter()
                    .flatten()
                    .filter(|id| next_voters.contains(id))
                    .collect();
                if next_voters.contains(&self.id) {
                    installed.push(self.id.clone());
                }
                if !state::has_quorum(next, &installed) {
                    return Err(QuorumError::UnknownOutcome);
                }
            }
            if candidate.command_id != command_id {
                continue;
            }
            return command_result(&candidate, &command, certificate);
        }
    }

    async fn maintenance(&self) {
        for (index, fragment) in self.fragments.iter().enumerate() {
            if self.closed.load(Ordering::Acquire) {
                return;
            }
            let local = fragment.replica.lock().await;
            let now = now_ms();
            let due = |snapshot: &wire::Snapshot| {
                snapshot.entries.iter().any(|e| {
                    e.record
                        .as_ref()
                        .is_some_and(|r| r.expires_at_ms.is_some_and(|t| t <= now))
                }) || snapshot.receipts.iter().any(|r| r.valid_until_ms <= now)
                    || snapshot.changes.first().is_some_and(|c| {
                        c.time_ms
                            .saturating_add(self.config.change_retention.as_millis() as u64)
                            < now
                    })
            };
            let needed = due(state::snapshot(state::committed(&local)))
                || local
                    .accepted
                    .as_ref()
                    .and_then(|p| p.snapshot.as_ref())
                    .is_some_and(due);
            drop(local);
            if needed {
                let _ = self
                    .route(wire::Command {
                        fragment: index as u32,
                        operation: Some(wire::command::Operation::Maintain(true)),
                    })
                    .await;
            }
        }
    }
}

fn targets_contains(targets: &[String], id: &str) -> bool {
    targets.iter().any(|target| target == id)
}

fn validate_successor(
    base: &wire::Snapshot,
    next: &wire::Snapshot,
    fragment: FragmentId,
    config: &QuorumConfig,
) -> Result<(), QuorumError> {
    state::validate_snapshot(next, fragment, config)?;
    if next.slot
        != base
            .slot
            .checked_add(1)
            .ok_or_else(|| QuorumError::ResourceLimit("revision exhausted".into()))?
        || next.time_ms < base.time_ms
        || next.last_change_id < base.last_change_id
        || next.change_floor < base.change_floor
    {
        return Err(QuorumError::Invalid("invalid snapshot successor".into()));
    }
    let old = base.membership.as_ref().expect("validated membership");
    let new = next.membership.as_ref().expect("validated membership");
    let valid = old == new
        || (old.next_voters.is_empty() && new.voters == old.voters && !new.next_voters.is_empty())
        || (!old.next_voters.is_empty()
            && new.voters == old.next_voters
            && new.next_voters.is_empty());
    if !valid {
        return Err(QuorumError::Invalid(
            "membership change omitted joint configuration".into(),
        ));
    }
    Ok(())
}

fn make_candidate(
    base: &wire::Snapshot,
    command: &wire::Command,
    command_id: &[u8],
    config: &QuorumConfig,
) -> Result<wire::Snapshot, QuorumError> {
    let mut candidate = base.clone();
    candidate.slot = base
        .slot
        .checked_add(1)
        .ok_or_else(|| QuorumError::ResourceLimit("revision exhausted".into()))?;
    candidate.command_id = command_id.to_vec();
    state::maintain(&mut candidate, config, now_ms())?;
    match command.operation.as_ref() {
        Some(wire::command::Operation::Mutation(mutation)) => {
            if fragment_of(&mutation.key, config.fragment_map.len()).0 != command.fragment {
                return Err(QuorumError::Invalid(
                    "mutation key fragment mismatch".into(),
                ));
            }
            state::apply_mutation(&mut candidate, mutation, config)?;
        }
        Some(wire::command::Operation::Get(key)) => {
            if fragment_of(key, config.fragment_map.len()).0 != command.fragment {
                return Err(QuorumError::Invalid("read key fragment mismatch".into()));
            }
        }
        Some(wire::command::Operation::Changes(request)) => {
            if request.limit == 0
                || request.limit as usize > config.max_changes
                || request.after > candidate.last_change_id
            {
                return Err(QuorumError::Invalid(
                    "invalid change cursor or limit".into(),
                ));
            }
            if request.after < candidate.change_floor {
                return Err(QuorumError::CursorExpired);
            }
        }
        Some(wire::command::Operation::Maintain(_)) => {}
        Some(wire::command::Operation::Snapshot(_)) => {}
        Some(wire::command::Operation::Reconfigure(request)) => {
            let desired = Membership {
                voters: request.voters.clone(),
                next_voters: vec![],
            };
            state::validate_membership(&desired)?;
            if desired
                .voters
                .iter()
                .any(|id| !config.members.contains_key(id))
            {
                return Err(QuorumError::Invalid(
                    "prospective voter missing from address book".into(),
                ));
            }
            let current = candidate.membership.as_mut().expect("validated membership");
            if current == &desired {
                return Ok(candidate);
            }
            if request.finish {
                if current.next_voters != desired.voters {
                    return Err(QuorumError::Invalid(
                        "joint membership does not match target".into(),
                    ));
                }
                *current = desired;
            } else if current.next_voters.is_empty() {
                current.next_voters = desired.voters;
            } else if current.next_voters != desired.voters {
                return Err(QuorumError::Invalid(
                    "another membership transition is active".into(),
                ));
            }
        }
        None => return Err(QuorumError::Invalid("missing command".into())),
    }
    if candidate.encoded_len() > config.max_state_bytes / 3 {
        return Err(QuorumError::ResourceLimit(
            "snapshot bytes including retry receipts".into(),
        ));
    }
    Ok(candidate)
}

fn command_result(
    snapshot: &wire::Snapshot,
    command: &wire::Command,
    commit: wire::Certificate,
) -> Result<wire::Response, QuorumError> {
    let mut response = wire::Response {
        commit: Some(commit),
        last_change_id: snapshot.last_change_id,
        ..Default::default()
    };
    match command.operation.as_ref() {
        Some(wire::command::Operation::Mutation(mutation)) => {
            response.mutation = snapshot
                .receipts
                .iter()
                .find(|r| r.request_id == mutation.request_id)
                .and_then(|r| r.result.clone());
        }
        Some(wire::command::Operation::Get(key)) => {
            response.record = snapshot
                .entries
                .binary_search_by(|entry| entry.key.cmp(key))
                .ok()
                .and_then(|i| snapshot.entries[i].record.clone());
        }
        Some(wire::command::Operation::Changes(request)) => {
            response.changes = snapshot
                .changes
                .iter()
                .filter(|c| c.id > request.after)
                .take(request.limit as usize)
                .cloned()
                .collect();
        }
        _ => {}
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::super::FileQuorumStore;
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn persisted_promise_rejects_old_prepare_and_accept_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(FileQuorumStore::open(directory.path()).unwrap());
        let config = QuorumConfig::builder()
            .cluster_id("fencing")
            .store(store)
            .fragment_map(HashMap::from([(
                FragmentId(0),
                vec!["a".into(), "b".into(), "c".into()],
            )]))
            .members(HashMap::from([
                ("a".into(), "127.0.0.1:10001".parse().unwrap()),
                ("b".into(), "127.0.0.1:10002".parse().unwrap()),
                ("c".into(), "127.0.0.1:10003".parse().unwrap()),
            ]))
            .build();
        let node = QuorumNode::start("b".into(), "127.0.0.1:0".parse().unwrap(), config.clone())
            .await
            .unwrap();
        let base = state::committed(&*node.core.fragments[0].replica.lock().await).clone();
        let request = |round| wire::Request {
            cluster_id: "fencing".into(),
            node_id: "a".into(),
            fragment: 0,
            operation: Some(wire::request::Operation::Prepare(wire::Prepare {
                base: Some(base.clone()),
                ballot: Some(wire::Ballot {
                    round,
                    node_id: "a".into(),
                }),
            })),
        };
        assert_eq!(
            node.core.protocol(request(10)).await.unwrap().status,
            wire::Status::Ok as i32
        );
        node.shutdown().await.unwrap();
        let restarted = QuorumNode::start("b".into(), "127.0.0.1:0".parse().unwrap(), config)
            .await
            .unwrap();
        let response = restarted.core.protocol(request(9)).await.unwrap();
        assert_eq!(response.status, wire::Status::Rejected as i32);
        assert_eq!(response.state.unwrap().promise.unwrap().round, 10);
        let candidate = make_candidate(
            state::snapshot(&base),
            &wire::Command {
                fragment: 0,
                operation: Some(wire::command::Operation::Maintain(true)),
            },
            &[1; 16],
            &restarted.core.config,
        )
        .unwrap();
        let response = restarted
            .core
            .protocol(wire::Request {
                cluster_id: "fencing".into(),
                node_id: "a".into(),
                fragment: 0,
                operation: Some(wire::request::Operation::Accept(wire::Accept {
                    base: Some(base),
                    proposal: Some(wire::Proposal {
                        ballot: Some(wire::Ballot {
                            round: 9,
                            node_id: "a".into(),
                        }),
                        snapshot: Some(candidate),
                    }),
                })),
            })
            .await
            .unwrap();
        assert_eq!(response.status, wire::Status::Rejected as i32);
        assert!(response.state.unwrap().accepted.is_none());
        restarted.shutdown().await.unwrap();
    }
}
