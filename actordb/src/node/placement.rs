// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Placement polling, fragment gain, and replication-fenced handoff.
//!
//! The per-fragment mutation write lease fences serving revocation and final
//! teardown. Placement/serving snapshots use short standard locks; sender and
//! supervisor registries are released before actor RPCs and report waits.
//! Placement generation, dispatcher generation, and exact sender identity
//! prevent stale gains or handoffs from publishing after an ABA transition.

use super::*;

/// Everything the Phase 8 placement driver needs. All shared
/// ownership; `Clone` for spawning per-fragment gain/lose tasks.
#[derive(Clone)]
pub(super) struct PlacementDriverDeps {
    pub(super) expiry_publisher: Option<ExpiryPublisher>,
    pub(super) node_id: String,
    pub(super) controller: Arc<dyn PlacementController>,
    pub(super) poll_interval: Duration,
    pub(super) drain_warn_interval: Duration,
    pub(super) fragment_count: usize,
    pub(super) subtree_config: NodeConfig,
    pub(super) router: FragmentRouter,
    pub(super) serving: Arc<RwLock<HashSet<FragmentId>>>,
    pub(super) placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
    pub(super) supervisors: Arc<Mutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>>,
    pub(super) supervisor_stops: Arc<SupervisorRegistry>,
    pub(super) placement_states: Arc<Mutex<HashMap<FragmentId, FragmentPlacementState>>>,
    /// Subtrees loaded synchronously during dynamic startup but not yet
    /// reconciled or published as authoritative. The first placement tick
    /// consumes this set into explicit gain/loss transitions.
    pub(super) preloaded_fragments: Arc<StdMutex<HashSet<FragmentId>>>,
    /// Increments only when the published placement map actually changes.
    /// A global generation is conservative: unrelated fragment movement can
    /// retry an in-flight bootstrap, but a P -> Q -> P ABA cannot publish work
    /// completed against the first P.
    pub(super) placement_generation: Arc<AtomicU64>,
    pub(super) senders: SenderMap,
    pub(super) finalizing_senders: Arc<SenderFinalizationRegistry>,
    pub(super) retiring_senders: Arc<RetiringSenderRegistry>,
    pub(super) sender_reports: SenderReportMap,
    pub(super) sender_config: SenderConfig,
    pub(super) sender_runtime: SenderRuntimeOptions,
    pub(super) peers: PeerMap,
    pub(super) peer_failures: PeerFailureSignals,
    /// Polling and gain/loss transitions, stopped to freeze placement before
    /// a replication-drained shutdown snapshots targets.
    pub(super) tasks: Weak<RuntimeTasks>,
    /// Dispatcher restart monitors are core actor lifecycle work and must stay
    /// alive while the frozen senders reconcile.
    pub(super) dispatcher_tasks: Weak<RuntimeTasks>,
    pub(super) operation_gate: Arc<OperationGate>,
    pub(super) hlc: Arc<Mutex<HlcClock>>,
    pub(super) recovery: Option<Arc<RecoveryContext>>,
}

impl PlacementDriverDeps {
    /// Idempotent sender spawn for (`fragment`, `peer`).
    pub(super) async fn ensure_sender(&self, fragment: FragmentId, peer: &str) {
        ensure_sender_entry(
            fragment,
            peer,
            SenderEntryDeps {
                config: &self.sender_config,
                runtime: &self.sender_runtime,
                router: &self.router,
                hlc: &self.hlc,
                senders: &self.senders,
                finalizing_senders: &self.finalizing_senders,
                retiring_senders: &self.retiring_senders,
                reports: &self.sender_reports,
                peer_failures: &self.peer_failures,
                operation_gate: &self.operation_gate,
                allow_quiescing: false,
            },
        )
        .await;
    }

    /// Completed reconcile rounds for (`fragment`, `peer`), if the
    /// sender exists.
    pub(super) async fn sender_rounds(&self, fragment: FragmentId, peer: &str) -> Option<u64> {
        let r = {
            let reports = self.sender_reports.lock().await;
            reports.get(&(fragment, peer.to_string())).cloned()?
        };
        let guard = r.lock().await;
        Some(guard.reconcile_rounds)
    }
}

/// Phase 8 placement driver: polls the controller and converges local
/// state (fragment subtrees, senders, routing) toward the desired
/// placement. Also wakes on membership change.
pub(super) async fn placement_driver(
    deps: PlacementDriverDeps,
    mut membership_rx: watch::Receiver<u64>,
) {
    // Converge immediately at startup; then on interval / membership.
    placement_tick(&deps).await;
    let mut interval = tokio::time::interval(deps.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                placement_tick(&deps).await;
            }
            changed = membership_rx.changed() => {
                if changed.is_err() {
                    break; // node shutting down
                }
                let members: Vec<String> =
                    deps.peers.lock().await.keys().cloned().collect();
                deps.controller.on_membership_change(&members);
                placement_tick(&deps).await;
            }
        }
    }
}

/// One driver tick: publish the controller's map as the authoritative
/// routing table, then gain/lose fragments to match.
pub(super) async fn placement_tick(deps: &PlacementDriverDeps) {
    let desired = deps.controller.desired_placement();
    if let Err(e) = validate_placement(&desired, deps.fragment_count) {
        tracing::warn!(error = %e, "invalid desired placement; keeping current map");
        return;
    }
    let mine_desired: HashSet<FragmentId> = desired
        .iter()
        .filter(|(_, r)| r.iter().any(|id| id == &deps.node_id))
        .map(|(f, _)| *f)
        .collect();

    // `placement_states` is also the transition commit mutex. Publishing a
    // controller map and marking its gain/loss transitions in one critical
    // section prevents a stale drainer from validating the old map and then
    // tearing down state after a re-add has already been published.
    let (mine_current, in_flight, transitions) = {
        let mut states = deps.placement_states.lock().await;
        // Canonical lock order is placement_states -> placement. The sync map
        // guard never crosses an await.
        let mut published = deps.placement.write().unwrap();
        if *published != desired {
            *published = desired.clone();
            deps.placement_generation.fetch_add(1, Ordering::AcqRel);
        }
        drop(published);
        let mut current = HashSet::new();
        let mut flight = HashSet::new();
        let mut transitions = Vec::new();
        for (f, s) in states.iter() {
            match s {
                FragmentPlacementState::Stopped => {}
                FragmentPlacementState::Bootstrapping | FragmentPlacementState::Draining => {
                    current.insert(*f);
                    flight.insert(*f);
                }
                FragmentPlacementState::Serving => {
                    current.insert(*f);
                }
            }
        }

        // Dynamic startup loaded these subtrees synchronously for durable
        // validation but deliberately withheld authority. Consume each one
        // exactly once into a transition which adopts (rather than replaces)
        // the existing actor tree. A controller revocation before this first
        // tick becomes a loss transition instead of leaving an orphan.
        let preloaded = std::mem::take(&mut *deps.preloaded_fragments.lock().unwrap());
        for fragment in preloaded {
            current.insert(fragment);
            flight.insert(fragment);
            if mine_desired.contains(&fragment) {
                states.insert(fragment, FragmentPlacementState::Bootstrapping);
                transitions.push((fragment, true, true));
            } else {
                states.insert(fragment, FragmentPlacementState::Draining);
                transitions.push((fragment, false, true));
            }
        }

        // Mark transitions before spawning so the next tick cannot create a
        // duplicate while the child task is waiting to be scheduled.
        for f in mine_desired.difference(&current) {
            states.insert(*f, FragmentPlacementState::Bootstrapping);
            transitions.push((*f, true, false));
        }
        for f in current.difference(&mine_desired) {
            if flight.contains(f) {
                continue;
            }
            states.insert(*f, FragmentPlacementState::Draining);
            transitions.push((*f, false, false));
        }
        (current, flight, transitions)
    };

    // Register outside the placement-state commit mutex. The placement task manager
    // owns and joins transitions directly, so aborting the polling driver
    // cannot detach a gain/loss child during shutdown.
    for (fragment, gaining, preloaded) in transitions {
        let transition_deps = deps.clone();
        let transition = async move {
            if gaining {
                gain_fragment(&transition_deps, fragment, preloaded).await;
            } else {
                lose_fragment(&transition_deps, fragment).await;
            }
        };
        let registered = match deps.tasks.upgrade() {
            Some(tasks) => tasks.spawn(transition).await,
            None => false,
        };
        if !registered {
            tracing::debug!(
                fragment = fragment.0,
                "node stopped before placement transition registered"
            );
        }
    }

    // Steady state: keep senders aligned with the replica sets.
    for f in mine_desired.intersection(&mine_current) {
        if in_flight.contains(f) {
            continue;
        }
        ensure_fragment_senders(deps, *f, &desired).await;
    }
}

/// Keep the sender set for a served fragment aligned with its replica
/// set: spawn missing senders, (re)connect them, prune senders for
/// peers that are no longer replicas.
pub(super) async fn ensure_fragment_senders(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    desired: &HashMap<FragmentId, Vec<String>>,
) {
    let replicas: Vec<String> = desired
        .get(&fragment)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|p| p != &deps.node_id)
        .collect();
    for peer in &replicas {
        deps.ensure_sender(fragment, peer).await;
    }
    // (Re)connect senders whose peers are currently connected. (The
    // Hello handler does this for new connections; the driver covers
    // senders spawned after the connection was established.)
    {
        let peers = deps.peers.lock().await;
        let targets: Vec<(NodeSender, BudgetedPeerTx)> = {
            let senders = deps.senders.lock().await;
            replicas
                .iter()
                .filter_map(|peer| {
                    Some((
                        senders.get(&(fragment, peer.clone()))?.clone(),
                        peers.get(peer)?.budgeted_sender(),
                    ))
                })
                .collect()
        };
        // Retain PeerMap's winner authority until every sender has observed
        // this channel, but do not retain the unrelated sender registry while
        // each task updates its own connection state.
        for (sender, peer_tx) in targets {
            let _ = sender.connect_peer(peer_tx);
        }
    }
    // Prune senders for peers that are no longer replicas. Their
    // buffered ops were fanned out to the remaining senders too, and
    // the peer's copy is not authoritative.
    let retiring = {
        // Placement publication is the authority for pruning. Re-read it
        // under its commit mutex so an older convergence pass cannot delete a
        // sender which a newer replica set has just made current.
        let _states = deps.placement_states.lock().await;
        let current_replicas: HashSet<String> = deps
            .placement
            .read()
            .unwrap()
            .get(&fragment)
            .into_iter()
            .flatten()
            .filter(|peer| *peer != &deps.node_id)
            .cloned()
            .collect();
        let mut senders = deps.senders.lock().await;
        let mut reports = deps.sender_reports.lock().await;
        let stale: Vec<(FragmentId, String)> = senders
            .keys()
            .filter(|(sender_fragment, peer)| {
                *sender_fragment == fragment && !current_replicas.contains(peer)
            })
            .cloned()
            .collect();
        let mut retiring = Vec::with_capacity(stale.len());
        for key in &stale {
            if let Some(sender) = senders.remove(key) {
                retiring.push(deps.retiring_senders.retire(sender));
            }
            reports.remove(key);
        }
        retiring
    };
    for sender in retiring {
        sender.join().await;
    }
}

/// Bootstrap timeout: bound the wait for a serving source peer and verified
/// reconcile round. Timeout keeps the fragment non-authoritative; the
/// placement loop can retry the gain later.
pub(super) const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FragmentGainCommit {
    Serving,
    Retry,
    Revoked,
}

/// Commit a completed gain only if the exact replica set which was reconciled
/// is still current. `placement_states` is the publication mutex used by
/// placement ticks, so this check and the Serving transition are atomic with
/// both revocation and replica-set replacement.
pub(super) async fn commit_fragment_gain(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    expected_replicas: &[String],
    expected_generation: u64,
) -> FragmentGainCommit {
    let mut states = deps.placement_states.lock().await;
    let current = deps
        .placement
        .read()
        .unwrap()
        .get(&fragment)
        .cloned()
        .unwrap_or_default();
    if !current.iter().any(|node| node == &deps.node_id) {
        states.insert(fragment, FragmentPlacementState::Draining);
        return FragmentGainCommit::Revoked;
    }
    if current != expected_replicas
        || deps.placement_generation.load(Ordering::Acquire) != expected_generation
    {
        states.insert(fragment, FragmentPlacementState::Bootstrapping);
        return FragmentGainCommit::Retry;
    }

    deps.serving.write().unwrap().insert(fragment);
    states.insert(fragment, FragmentPlacementState::Serving);
    FragmentGainCommit::Serving
}

/// Align replication senders and reconcile against one placement snapshot.
/// If placement changes while reconciliation is in flight, repeat against the
/// new replica set before publishing the fragment as authoritative.
pub(super) async fn converge_fragment_gain(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) -> bool {
    loop {
        let (desired, replicas, generation) = {
            let mut states = deps.placement_states.lock().await;
            let desired = deps.placement.read().unwrap().clone();
            let generation = deps.placement_generation.load(Ordering::Acquire);
            let replicas = desired.get(&fragment).cloned().unwrap_or_default();
            if !replicas.iter().any(|node| node == &deps.node_id) {
                states.insert(fragment, FragmentPlacementState::Draining);
                return false;
            }
            states.insert(fragment, FragmentPlacementState::Bootstrapping);
            (desired, replicas, generation)
        };

        if deps
            .recovery
            .as_ref()
            .is_some_and(|recovery| !recovery.matches_placement(fragment, &replicas))
        {
            tracing::warn!(
                fragment = fragment.0,
                "recovery placement changed; explicit new recovery authority required"
            );
            return false;
        }
        ensure_fragment_senders(deps, fragment, &desired).await;
        let sources: Vec<String> = replicas
            .iter()
            .filter(|peer| *peer != &deps.node_id)
            .cloned()
            .collect();
        let recovery_fence = if deps.recovery.is_some() {
            recovery::bootstrap_all(deps, fragment, &sources).await
        } else {
            None
        };
        let bootstrapped = if deps.recovery.is_some() {
            recovery_fence.is_some()
        } else {
            sources.is_empty() || bootstrap_from(deps, fragment, &sources).await
        };
        if !bootstrapped {
            tracing::warn!(
                fragment = fragment.0,
                "fragment bootstrap timed out; refusing to publish incomplete state"
            );
            return false;
        }

        match commit_fragment_gain(deps, fragment, &replicas, generation).await {
            FragmentGainCommit::Serving => return true,
            FragmentGainCommit::Retry => continue,
            FragmentGainCommit::Revoked => return false,
        }
    }
}

/// Gain a fragment: adopt a synchronously preloaded startup subtree or spawn a
/// new one, bootstrap state from a replica holder via the bounded reconciler,
/// then go Serving. The fragment is not authoritative until bootstrap
/// completes: local reads/writes forward to a serving owner meanwhile.
pub(super) async fn gain_fragment(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    preloaded: bool,
) {
    if preloaded {
        let actor_loaded = deps.router.route_for_fragment(fragment).is_some();
        let supervisor_loaded = deps.supervisors.lock().await.contains_key(&fragment);
        if !actor_loaded || !supervisor_loaded {
            tracing::error!(
                fragment = fragment.0,
                actor_loaded,
                supervisor_loaded,
                "dynamic startup preload was incomplete"
            );
            deps.placement_states.lock().await.remove(&fragment);
            return;
        }
    } else {
        let (supervisor, dispatcher) = match spawn_fragment_subtree_registered(
            &deps.node_id,
            fragment,
            &deps.subtree_config,
            &deps.hlc,
            &deps.supervisor_stops,
            deps.expiry_publisher.clone(),
        )
        .await
        {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(%fragment, error = %e, "failed to spawn placement subtree");
                // Drop the Bootstrapping mark so the next tick retries.
                deps.placement_states.lock().await.remove(&fragment);
                return;
            }
        };
        deps.supervisors
            .lock()
            .await
            .insert(fragment, supervisor.clone());
        // The actor exists in the router now (so the reconciler can merge
        // into it), but the fragment is not serving yet.
        let generation = deps.router.insert(fragment, dispatcher.clone());
        let mut route_registration = RouteMonitorRegistrationGuard {
            router: deps.router.clone(),
            fragment,
            generation,
            armed: true,
        };
        let monitor_registered = match deps.dispatcher_tasks.upgrade() {
            Some(tasks) => {
                register_fragment_dispatcher_monitor(
                    &tasks,
                    fragment,
                    generation,
                    supervisor.clone(),
                    deps.router.clone(),
                    dispatcher,
                )
                .await
            }
            None => false,
        };
        if !monitor_registered {
            drop(route_registration);
            deps.supervisors.lock().await.remove(&fragment);
            deps.placement_states
                .lock()
                .await
                .insert(fragment, FragmentPlacementState::Draining);
            match quiesce_fragment_supervisor(&supervisor).await {
                Ok(()) if deps.supervisor_stops.mark_prepared(fragment, &supervisor) => {
                    supervisor.stop(None);
                    let _ = supervisor.wait(None).await;
                    deps.supervisor_stops.remove_if(fragment, &supervisor);
                    deps.placement_states.lock().await.remove(&fragment);
                }
                Ok(()) => {
                    tracing::error!(
                        fragment = fragment.0,
                        "startup rollback lost exact supervisor ownership"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        fragment = fragment.0,
                        %error,
                        "startup rollback retained a fragment with an incomplete final flush"
                    );
                }
            }
            return;
        }
        route_registration.disarm();
    }

    if converge_fragment_gain(deps, fragment).await {
        deps.controller.on_fragment_ready(fragment);
    } else {
        lose_fragment(deps, fragment).await;
    }
}

/// Run one reconcile round against a source holder and wait for it to
/// complete. Returns false on timeout / no source.
pub(super) async fn bootstrap_from(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    sources: &[String],
) -> bool {
    let start = std::time::Instant::now();
    let mut baselines = HashMap::<String, u64>::new();
    // Trigger every connected candidate rather than trusting replica order.
    // A simultaneously bootstrapping replica is deliberately non-authoritative
    // and will reject the request; any already-serving holder may complete it.
    loop {
        let connected: Vec<String> = {
            let peers = deps.peers.lock().await;
            sources
                .iter()
                .filter(|source| peers.contains_key(*source))
                .cloned()
                .collect()
        };
        for source in connected {
            if baselines.contains_key(&source) {
                continue;
            }
            let Some(before) = deps.sender_rounds(fragment, &source).await else {
                continue;
            };
            let sender = deps
                .senders
                .lock()
                .await
                .get(&(fragment, source.clone()))
                .cloned();
            if let Some(sender) = sender {
                if sender.send(SenderMsg::ReconcileNow).is_ok() {
                    baselines.insert(source, before);
                }
            }
        }
        for (source, before) in &baselines {
            if deps
                .sender_rounds(fragment, source)
                .await
                .is_some_and(|rounds| rounds > *before)
            {
                return true;
            }
        }
        if start.elapsed() > BOOTSTRAP_TIMEOUT {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Clone)]
pub(super) struct FragmentSenderSnapshot {
    pub(super) key: (FragmentId, String),
    pub(super) sender: NodeSender,
    pub(super) report: Arc<Mutex<SenderStateReport>>,
}

pub(super) struct DrainedFragmentSenderSnapshot {
    pub(super) entries: Vec<FragmentSenderSnapshot>,
    pub(super) _report_guards: Vec<OwnedMutexGuard<SenderStateReport>>,
}

pub(super) fn snapshot_fragment_sender_set(
    fragment: FragmentId,
    senders: &HashMap<(FragmentId, String), NodeSender>,
    reports: &HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>,
) -> Option<Vec<FragmentSenderSnapshot>> {
    let mut entries: Vec<FragmentSenderSnapshot> = senders
        .iter()
        .filter(|((sender_fragment, _), _)| *sender_fragment == fragment)
        .map(|(key, sender)| {
            Some(FragmentSenderSnapshot {
                key: key.clone(),
                sender: sender.clone(),
                report: reports.get(key)?.clone(),
            })
        })
        .collect::<Option<_>>()?;
    let report_count = reports
        .keys()
        .filter(|(report_fragment, _)| *report_fragment == fragment)
        .count();
    if report_count != entries.len() {
        return None;
    }
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    Some(entries)
}

pub(super) async fn lock_drained_fragment_sender_snapshot(
    entries: Vec<FragmentSenderSnapshot>,
) -> Option<DrainedFragmentSenderSnapshot> {
    if entries
        .iter()
        .any(|entry| entry.sender.is_closed() || !entry.sender.ingress_is_idle())
    {
        return None;
    }
    let mut report_guards = Vec::with_capacity(entries.len());
    for entry in &entries {
        report_guards.push(entry.report.clone().lock_owned().await);
    }
    if entries
        .iter()
        .any(|entry| entry.sender.is_closed() || !entry.sender.ingress_is_idle())
        || report_guards.iter().any(|report| {
            report.buffered != 0 || report.in_flight != 0 || report.state == "buffer_reset"
        })
    {
        return None;
    }
    Some(DrainedFragmentSenderSnapshot {
        entries,
        _report_guards: report_guards,
    })
}

pub(super) fn fragment_sender_snapshot_is_current(
    fragment: FragmentId,
    snapshot: &[FragmentSenderSnapshot],
    senders: &HashMap<(FragmentId, String), NodeSender>,
    reports: &HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>,
) -> bool {
    senders
        .keys()
        .filter(|(sender_fragment, _)| *sender_fragment == fragment)
        .count()
        == snapshot.len()
        && reports
            .keys()
            .filter(|(report_fragment, _)| *report_fragment == fragment)
            .count()
            == snapshot.len()
        && snapshot.iter().all(|entry| {
            senders
                .get(&entry.key)
                .is_some_and(|sender| sender.same_task(&entry.sender))
                && reports
                    .get(&entry.key)
                    .is_some_and(|report| Arc::ptr_eq(report, &entry.report))
        })
}

#[cfg(test)]
pub(super) async fn fragment_sender_set_is_drained(
    fragment: FragmentId,
    senders: &HashMap<(FragmentId, String), NodeSender>,
    reports: &HashMap<(FragmentId, String), Arc<Mutex<SenderStateReport>>>,
) -> bool {
    let Some(snapshot) = snapshot_fragment_sender_set(fragment, senders, reports) else {
        return false;
    };
    lock_drained_fragment_sender_snapshot(snapshot)
        .await
        .is_some()
}

pub(super) async fn restore_fragment_if_readded(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) -> bool {
    // Remain non-authoritative while the sender set is realigned and the
    // retained subtree catches up on writes forwarded during Draining.
    let restored = converge_fragment_gain(deps, fragment).await;
    if restored {
        deps.controller.on_fragment_ready(fragment);
    }
    restored
}

pub(super) struct FragmentHandoffDrain {
    pub(super) peer: String,
    pub(super) sender: NodeSender,
    pub(super) drain: NodeSenderDrain,
}

pub(super) fn fragment_placement_snapshot(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) -> (HashMap<FragmentId, Vec<String>>, Vec<String>, u64) {
    let desired = deps.placement.read().unwrap().clone();
    let replicas = desired.get(&fragment).cloned().unwrap_or_default();
    let generation = deps.placement_generation.load(Ordering::Acquire);
    (desired, replicas, generation)
}

pub(super) fn fragment_placement_matches(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    expected_replicas: &[String],
    expected_generation: u64,
) -> bool {
    deps.placement
        .read()
        .unwrap()
        .get(&fragment)
        .is_some_and(|replicas| replicas == expected_replicas)
        && deps.placement_generation.load(Ordering::Acquire) == expected_generation
}

pub(super) async fn routed_fragment_revision(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) -> Result<u64, String> {
    let actor = deps
        .router
        .route_for_fragment(fragment)
        .ok_or_else(|| "fragment route is absent".to_string())?;
    match actor
        .call(
            |reply| KvMessage::GetRevision { reply },
            Some(REMOTE_RPC_TIMEOUT),
        )
        .await
        .map_err(|error| error.to_string())?
    {
        CallResult::Success(revision) => Ok(revision),
        CallResult::Timeout => Err("fragment revision query timed out".to_string()),
        CallResult::SenderError => Err("fragment revision reply channel dropped".to_string()),
    }
}

/// Remove and join every sender for one fragment. Placement handoff drains
/// permanently seal sender ingress, so any retry or re-add must install fresh
/// sender tasks before it can publish the fragment again.
pub(super) async fn retire_fragment_senders(deps: &PlacementDriverDeps, fragment: FragmentId) {
    let retiring = {
        let mut senders = deps.senders.lock().await;
        let mut reports = deps.sender_reports.lock().await;
        let keys: Vec<(FragmentId, String)> = senders
            .keys()
            .filter(|(sender_fragment, _)| *sender_fragment == fragment)
            .cloned()
            .collect();
        let mut retiring = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(sender) = senders.remove(&key) {
                retiring.push(deps.retiring_senders.retire(sender));
            }
            reports.remove(&key);
        }
        retiring
    };
    for sender in retiring {
        sender.join().await;
    }
}

pub(super) async fn restore_fragment_after_handoff_cancelled(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) -> bool {
    retire_fragment_senders(deps, fragment).await;
    restore_fragment_if_readded(deps, fragment).await
}

pub(super) async fn finish_fragment_prefix_retirement(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
    supervisor: ActorRef<SupervisorMessage>,
    expected_replicas: Vec<String>,
    expected_generation: u64,
) {
    // The router is irreversibly detached, but the registry keeps the
    // quiesced cache and retryable persistence batch alive. Flush before
    // deletion so no later actor write can recreate the retired namespace.
    let mut next_flush_warning = std::time::Instant::now();
    loop {
        match quiesce_fragment_supervisor(&supervisor).await {
            Ok(()) => {
                if deps.supervisor_stops.mark_prepared(fragment, &supervisor) {
                    break;
                }
                tracing::error!(
                    fragment = fragment.0,
                    "fragment quiesced but supervisor ownership changed"
                );
                return;
            }
            Err(error) => {
                if std::time::Instant::now() >= next_flush_warning {
                    tracing::warn!(
                        fragment = fragment.0,
                        %error,
                        "fragment remains Draining while its final local flush is incomplete"
                    );
                    next_flush_warning = std::time::Instant::now() + deps.drain_warn_interval;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    let base_store = deps
        .subtree_config
        .store
        .as_ref()
        .expect("prefix retirement requires a configured store")
        .clone();
    let fragment_store = Arc::new(PrefixedStore::new(base_store, fragment));
    let mut next_delete_warning = std::time::Instant::now();
    loop {
        // Placement publication uses this same mutex. Holding it through the
        // synchronous, idempotent delete makes the final revoked-map check and
        // namespace mutation one atomic lifecycle decision.
        let states = deps.placement_states.clone().lock_owned().await;
        if !fragment_placement_matches(deps, fragment, &expected_replicas, expected_generation) {
            tracing::info!(
                fragment = fragment.0,
                "retaining quiesced fragment prefix because placement changed after handoff"
            );
            drop(states);
            break;
        }
        let backend = fragment_store.clone();
        let deleted = deps
            .subtree_config
            .persistence
            .executor()
            .run(move || {
                let _placement_guard = states;
                backend.delete_prefix(&[])
            })
            .await;
        match deleted {
            Ok(PrefixDeleteOutcome::Deleted) => {
                tracing::info!(fragment = fragment.0, "retired durable fragment prefix");
                break;
            }
            Ok(PrefixDeleteOutcome::Retained) => {
                tracing::info!(
                    fragment = fragment.0,
                    "store retained fragment prefix after retirement request"
                );
                break;
            }
            Err(error) => {
                if std::time::Instant::now() >= next_delete_warning {
                    tracing::warn!(
                        fragment = fragment.0,
                        %error,
                        "fragment remains Draining while durable prefix deletion is incomplete"
                    );
                    next_delete_warning = std::time::Instant::now() + deps.drain_warn_interval;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    supervisor.stop(None);
    let _ = supervisor.wait(None).await;
    if !deps.supervisor_stops.remove_if(fragment, &supervisor) {
        tracing::error!(
            fragment = fragment.0,
            "fragment stopped but exact supervisor ownership was lost"
        );
        return;
    }
    deps.placement_states
        .lock()
        .await
        .insert(fragment, FragmentPlacementState::Stopped);
}

pub(super) async fn lose_fragment(deps: &PlacementDriverDeps, fragment: FragmentId) {
    let prefix_delete_support = deps
        .subtree_config
        .store
        .as_ref()
        .map_or(PrefixDeleteSupport::Retained, |store| {
            store.prefix_delete_support()
        });
    match prefix_delete_support {
        PrefixDeleteSupport::Supported => {
            lose_fragment_with_prefix_retirement(deps, fragment).await
        }
        PrefixDeleteSupport::Retained => lose_fragment_retaining_prefix(deps, fragment).await,
    }
}

/// Retire a persisted fragment only after every replacement replica proves a
/// stable LWW floor from one unchanged source revision. Placement changes,
/// fetched winners, or sender replacement invalidate the attempt and rebuild
/// its sealed sender set from scratch. TTL cleanup may independently
/// materialize an equivalent tombstone after that revision pin.
pub(super) async fn lose_fragment_with_prefix_retirement(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) {
    let mutation_gate = deps.router.mutation_gate(fragment);
    let mutation_revocation = mutation_gate.write_owned().await;
    deps.serving.write().unwrap().remove(&fragment);
    drop(mutation_revocation);

    let mut next_warning = std::time::Instant::now() + deps.drain_warn_interval;
    loop {
        let (desired, replicas, generation) = fragment_placement_snapshot(deps, fragment);
        if replicas.iter().any(|node| node == &deps.node_id) {
            if restore_fragment_after_handoff_cancelled(deps, fragment).await {
                return;
            }
            continue;
        }
        let targets: Vec<String> = replicas
            .iter()
            .filter(|peer| *peer != &deps.node_id)
            .cloned()
            .collect();
        if targets.is_empty() {
            if std::time::Instant::now() >= next_warning {
                tracing::warn!(
                    fragment = fragment.0,
                    "fragment remains Draining because no replacement replica can certify handoff"
                );
                next_warning = std::time::Instant::now() + deps.drain_warn_interval;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        ensure_fragment_senders(deps, fragment, &desired).await;
        let baseline_revision = match routed_fragment_revision(deps, fragment).await {
            Ok(revision) => revision,
            Err(error) => {
                tracing::warn!(fragment = fragment.0, %error, "cannot read handoff baseline");
                retire_fragment_senders(deps, fragment).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let target_senders: Option<Vec<(String, NodeSender)>> = {
            let senders = deps.senders.lock().await;
            targets
                .iter()
                .map(|peer| {
                    senders
                        .get(&(fragment, peer.clone()))
                        .cloned()
                        .map(|sender| (peer.clone(), sender))
                })
                .collect()
        };
        let Some(target_senders) = target_senders else {
            retire_fragment_senders(deps, fragment).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let mut waiters = Vec::with_capacity(target_senders.len());
        for (peer, sender) in target_senders {
            let Ok(drain) = sender.request_drain_at(ReplicationAckDurability::StableCommitted)
            else {
                waiters.clear();
                break;
            };
            waiters.push(FragmentHandoffDrain {
                peer,
                sender,
                drain,
            });
        }
        if waiters.len() != targets.len() {
            retire_fragment_senders(deps, fragment).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let certified = loop {
            if !fragment_placement_matches(deps, fragment, &replicas, generation) {
                break false;
            }
            if waiters.iter().all(|waiter| waiter.drain.is_complete()) {
                break true;
            }
            if waiters
                .iter()
                .any(|waiter| waiter.drain.is_closed() || waiter.sender.is_closed())
            {
                break false;
            }
            if std::time::Instant::now() >= next_warning {
                let pending: Vec<&str> = waiters
                    .iter()
                    .filter(|waiter| !waiter.drain.is_complete())
                    .map(|waiter| waiter.peer.as_str())
                    .collect();
                tracing::warn!(
                    fragment = fragment.0,
                    ?pending,
                    "fragment remains Draining while stable handoff is incomplete"
                );
                next_warning = std::time::Instant::now() + deps.drain_warn_interval;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        if !certified {
            if replicas.iter().any(|node| node == &deps.node_id)
                && restore_fragment_after_handoff_cancelled(deps, fragment).await
            {
                return;
            }
            retire_fragment_senders(deps, fragment).await;
            continue;
        }

        // Point-of-no-return commit for this attempt. Fence only this
        // fragment's mutations, then atomically seal and detach the exact
        // certified sender generation. The seal is ordered by SenderMap, so a
        // queued ensure cannot recreate a task while the map is unlocked for
        // joins and the final actor revision query.
        let mutation_commit = deps.router.mutation_gate(fragment).write_owned().await;
        let states = deps.placement_states.lock().await;
        if !fragment_placement_matches(deps, fragment, &replicas, generation) {
            drop(states);
            drop(mutation_commit);
            retire_fragment_senders(deps, fragment).await;
            continue;
        }
        let mut senders = deps.senders.lock().await;
        let mut reports = deps.sender_reports.lock().await;
        let exact_senders = senders
            .iter()
            .filter(|((sender_fragment, _), _)| *sender_fragment == fragment)
            .count()
            == waiters.len()
            && waiters.iter().all(|waiter| {
                waiter.drain.is_complete()
                    && senders
                        .get(&(fragment, waiter.peer.clone()))
                        .is_some_and(|sender| sender.same_task(&waiter.sender))
                    && reports.contains_key(&(fragment, waiter.peer.clone()))
            })
            && reports
                .keys()
                .filter(|(report_fragment, _)| *report_fragment == fragment)
                .count()
                == waiters.len();
        if !exact_senders {
            drop(reports);
            drop(senders);
            drop(mutation_commit);
            drop(states);
            retire_fragment_senders(deps, fragment).await;
            continue;
        }
        let Some(finalization) = deps.finalizing_senders.seal(fragment) else {
            drop(reports);
            drop(senders);
            drop(states);
            drop(mutation_commit);
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };

        let mut retiring_senders = Vec::with_capacity(waiters.len());
        for waiter in &waiters {
            let key = (fragment, waiter.peer.clone());
            if let Some(sender) = senders.remove(&key) {
                retiring_senders.push(deps.retiring_senders.retire(sender));
            }
            reports.remove(&key);
        }
        drop(reports);
        drop(senders);
        drop(states);

        for sender in retiring_senders {
            sender.join().await;
        }
        let final_revision = routed_fragment_revision(deps, fragment).await;
        if final_revision != Ok(baseline_revision) {
            tracing::debug!(
                fragment = fragment.0,
                baseline_revision,
                ?final_revision,
                "fragment changed during handoff; retrying every replacement replica"
            );
            drop(finalization);
            drop(mutation_commit);
            continue;
        }

        // Placement publication remained available during the potentially
        // slow joins/RPC. Revalidate its exact generation before committing
        // route removal; P -> Q -> P is rejected by placement_generation.
        let states = deps.placement_states.lock().await;
        if !fragment_placement_matches(deps, fragment, &replicas, generation) {
            drop(states);
            drop(finalization);
            drop(mutation_commit);
            continue;
        }
        let senders = deps.senders.lock().await;
        let reports = deps.sender_reports.lock().await;
        let sender_set_remains_detached = !senders
            .keys()
            .any(|(sender_fragment, _)| *sender_fragment == fragment)
            && !reports
                .keys()
                .any(|(report_fragment, _)| *report_fragment == fragment);
        if !sender_set_remains_detached {
            drop(reports);
            drop(senders);
            drop(states);
            drop(finalization);
            drop(mutation_commit);
            continue;
        }

        let mut active_supervisors = deps.supervisors.lock().await;
        let active_supervisor = active_supervisors.get(&fragment).cloned();
        let registered_supervisor = deps.supervisor_stops.get(fragment);
        let ownership_matches = match (&active_supervisor, &registered_supervisor) {
            (Some(active), Some(registered)) => active.get_id() == registered.actor.get_id(),
            _ => false,
        };
        if !ownership_matches {
            drop(active_supervisors);
            drop(reports);
            drop(senders);
            drop(states);
            drop(finalization);
            drop(mutation_commit);
            tracing::error!(
                fragment = fragment.0,
                "refusing to destroy fragment without exact supervisor ownership"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let supervisor = active_supervisors
            .remove(&fragment)
            .expect("ownership checked above");
        deps.router.remove(fragment);
        drop(active_supervisors);
        drop(reports);
        drop(senders);
        drop(states);
        drop(finalization);
        drop(mutation_commit);

        finish_fragment_prefix_retirement(deps, fragment, supervisor, replicas, generation).await;
        return;
    }
}

/// Lose a fragment: stop serving new local ops (they forward to the
/// new owner), drain replication senders, then stop the subtree.
pub(super) async fn lose_fragment_retaining_prefix(
    deps: &PlacementDriverDeps,
    fragment: FragmentId,
) {
    // First wait for every mutation which already chose this owner to finish
    // both its local apply and sender fan-out. New mutations queue behind this
    // exclusive lease, observe `serving` removed, and forward to the new owner.
    let mutation_gate = deps.router.mutation_gate(fragment);
    let mutation_revocation = mutation_gate.write_owned().await;
    deps.serving.write().unwrap().remove(&fragment);
    drop(mutation_revocation);
    // Keep senders to the current replicas so queued ops drain out.
    let replicas: Vec<String> = deps
        .placement
        .read()
        .unwrap()
        .get(&fragment)
        .cloned()
        .unwrap_or_default();
    for peer in &replicas {
        if peer != &deps.node_id {
            deps.ensure_sender(fragment, peer).await;
        }
    }
    {
        let peers = deps.peers.lock().await;
        let targets: Vec<(NodeSender, BudgetedPeerTx)> = deps
            .senders
            .lock()
            .await
            .iter()
            .filter_map(|((sender_fragment, peer), sender)| {
                (*sender_fragment == fragment)
                    .then(|| {
                        peers
                            .get(peer)
                            .map(|tx| (sender.clone(), tx.budgeted_sender()))
                    })
                    .flatten()
            })
            .collect();
        for (sender, peer_tx) in targets {
            let _ = sender.connect_peer(peer_tx);
        }
    }
    // Wait until every copy has actually drained. A slow or partitioned peer
    // leaves this fragment in Draining; operators may repair placement or
    // connectivity without the old owner destroying its recovery state.
    let mut next_warning = std::time::Instant::now() + deps.drain_warn_interval;
    loop {
        if restore_fragment_if_readded(deps, fragment).await {
            // The controller reversed the move while it was draining. Restore
            // local service instead of letting the stale transition remove a
            // newly authoritative fragment.
            return;
        }

        let sender_snapshot = {
            let senders = deps.senders.lock().await;
            let reports = deps.sender_reports.lock().await;
            snapshot_fragment_sender_set(fragment, &senders, &reports)
        };
        let drained = match sender_snapshot {
            Some(snapshot) => lock_drained_fragment_sender_snapshot(snapshot)
                .await
                .is_some(),
            None => false,
        };
        if drained {
            // Final teardown commit. `placement_states` serializes this
            // validation with placement publication. Acquire the per-fragment
            // mutation fence before any global registry, then await report
            // state without those registries held.
            let mutation_commit = deps.router.mutation_gate(fragment).write_owned().await;
            let final_snapshot = {
                let senders = deps.senders.lock().await;
                let reports = deps.sender_reports.lock().await;
                snapshot_fragment_sender_set(fragment, &senders, &reports)
            };
            let Some(final_snapshot) = final_snapshot else {
                drop(mutation_commit);
                continue;
            };
            let Some(drained) = lock_drained_fragment_sender_snapshot(final_snapshot).await else {
                drop(mutation_commit);
                continue;
            };

            let mut states = deps.placement_states.lock().await;
            let still_revoked = deps
                .placement
                .read()
                .unwrap()
                .get(&fragment)
                .is_none_or(|replicas| !replicas.iter().any(|node| node == &deps.node_id));
            if !still_revoked {
                // A re-add raced the final drain check. Keep the subtree
                // non-serving and let the top of the loop realign senders and
                // reconcile before publishing it again.
                states.insert(fragment, FragmentPlacementState::Bootstrapping);
                drop(states);
                drop(drained);
                drop(mutation_commit);
                continue;
            }

            let mut senders = deps.senders.lock().await;
            let mut reports = deps.sender_reports.lock().await;
            if !fragment_sender_snapshot_is_current(fragment, &drained.entries, &senders, &reports)
            {
                drop(reports);
                drop(senders);
                drop(states);
                drop(drained);
                drop(mutation_commit);
                continue;
            }

            // Detach every admission/recreation path before the first teardown
            // wait. A queued ensure call cannot acquire `senders` until after
            // the router is absent, and an inbound mutation cannot acquire its
            // shared lease until after the same point.
            let mut active_supervisors = deps.supervisors.lock().await;
            let active_supervisor = active_supervisors.get(&fragment).cloned();
            let registered_supervisor = deps.supervisor_stops.get(fragment);
            let ownership_matches = match (&active_supervisor, &registered_supervisor) {
                (Some(active), Some(registered)) => active.get_id() == registered.actor.get_id(),
                _ => false,
            };
            if !ownership_matches {
                drop(active_supervisors);
                drop(reports);
                drop(senders);
                drop(states);
                drop(drained);
                drop(mutation_commit);
                tracing::error!(
                    fragment = fragment.0,
                    "refusing to destroy fragment without exact supervisor ownership"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let supervisor = active_supervisors
                .remove(&fragment)
                .expect("ownership checked above");
            let mut retiring_senders = Vec::with_capacity(drained.entries.len());
            for entry in &drained.entries {
                if let Some(sender) = senders.remove(&entry.key) {
                    retiring_senders.push(deps.retiring_senders.retire(sender));
                }
                reports.remove(&entry.key);
            }
            drop(active_supervisors);
            deps.router.remove(fragment);
            drop(reports);
            drop(senders);
            drop(states);
            drop(drained);
            drop(mutation_commit);

            for sender in retiring_senders {
                sender.join().await;
            }

            // The router is now irreversibly detached, but the registry keeps
            // the quiesced cache and retryable persistence batch alive. A
            // store outage leaves placement visibly Draining; a re-add waits
            // for this teardown to finish and starts a fresh, durable subtree.
            let mut next_flush_warning = std::time::Instant::now();
            loop {
                match quiesce_fragment_supervisor(&supervisor).await {
                    Ok(()) => {
                        if deps.supervisor_stops.mark_prepared(fragment, &supervisor) {
                            break;
                        }
                        tracing::error!(
                            fragment = fragment.0,
                            "fragment quiesced but supervisor ownership changed"
                        );
                        return;
                    }
                    Err(error) => {
                        if std::time::Instant::now() >= next_flush_warning {
                            tracing::warn!(
                                fragment = fragment.0,
                                %error,
                                "fragment remains Draining while its final local flush is incomplete"
                            );
                            next_flush_warning =
                                std::time::Instant::now() + deps.drain_warn_interval;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            supervisor.stop(None);
            let _ = supervisor.wait(None).await;
            if !deps.supervisor_stops.remove_if(fragment, &supervisor) {
                tracing::error!(
                    fragment = fragment.0,
                    "fragment stopped but exact supervisor ownership was lost"
                );
                return;
            }
            deps.placement_states
                .lock()
                .await
                .insert(fragment, FragmentPlacementState::Stopped);
            return;
        }
        if std::time::Instant::now() >= next_warning {
            tracing::warn!(
                fragment = fragment.0,
                "fragment remains Draining while replication recovery is incomplete"
            );
            next_warning = std::time::Instant::now() + deps.drain_warn_interval;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl Node {
    /// Phase 8 placement report: this node's authoritative placement
    /// map plus per-fragment lifecycle state (Serving, Bootstrapping,
    /// Draining, Stopped).
    pub async fn placement_report(&self) -> PlacementReport {
        let placement = self.fragment_map.read().unwrap().clone();
        let states = self.placement_states.lock().await.clone();
        PlacementReport {
            placement,
            states,
            dynamic: self.placement_controller.is_some(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn placement_revocation_waits_for_admitted_mutation_lease() {
        let router = FragmentRouter::new(1, 0);
        let fragment = FragmentId(0);
        let serving = Arc::new(RwLock::new(HashSet::from([fragment])));
        let admitted = router.mutation_gate(fragment).read_owned().await;
        let revocation_gate = router.mutation_gate(fragment);
        let revoked_serving = serving.clone();
        let revoke = tokio::spawn(async move {
            let exclusive = revocation_gate.write_owned().await;
            revoked_serving.write().unwrap().remove(&fragment);
            drop(exclusive);
        });
        tokio::task::yield_now().await;
        assert!(!revoke.is_finished());
        assert!(serving.read().unwrap().contains(&fragment));

        drop(admitted);
        revoke.await.unwrap();
        assert!(!serving.read().unwrap().contains(&fragment));
        let _late_mutation = router.mutation_gate(fragment).read_owned().await;
        assert!(!serving.read().unwrap().contains(&fragment));
    }

    #[tokio::test]
    async fn placement_drain_rechecks_reset_required_sender_as_one_map_snapshot() {
        let fragment = FragmentId(0);
        let report = Arc::new(Mutex::new(SenderStateReport {
            state: "disconnected".to_string(),
            session_id: 0,
            buffered: 0,
            retained_bytes: 0,
            aggregate_retained_bytes: 0,
            in_flight: 0,
            last_ack_durability: None,
            ack_durability_mismatch: false,
            reconcile_rounds: 0,
        }));
        let sender = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        sender.schedule_reset();
        let key = (fragment, "peer".to_string());
        let senders = HashMap::from([(key.clone(), sender.clone())]);
        let reports = HashMap::from([(key, report)]);

        assert!(!fragment_sender_set_is_drained(fragment, &senders, &reports).await);
        sender.stop().await;
    }

    #[tokio::test]
    async fn completed_bootstrap_cannot_publish_after_placement_revocation() {
        let fragment = FragmentId(0);
        let serving = Arc::new(RwLock::new(HashSet::new()));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["node-b".to_string()],
        )])));
        let deps = test_placement_deps(
            FragmentRouter::new(1, 0),
            serving.clone(),
            placement,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );
        deps.placement_states
            .lock()
            .await
            .insert(fragment, FragmentPlacementState::Bootstrapping);

        let expected = vec!["node-a".to_string(), "node-b".to_string()];
        assert_eq!(
            commit_fragment_gain(&deps, fragment, &expected, 0).await,
            FragmentGainCommit::Revoked
        );
        assert!(!serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Draining)
        );
    }

    #[tokio::test]
    async fn completed_bootstrap_retries_after_replica_set_replacement() {
        let fragment = FragmentId(0);
        let serving = Arc::new(RwLock::new(HashSet::new()));
        let current = vec!["node-a".to_string(), "node-c".to_string()];
        let placement = Arc::new(RwLock::new(HashMap::from([(fragment, current.clone())])));
        let deps = test_placement_deps(
            FragmentRouter::new(1, 0),
            serving.clone(),
            placement,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );
        deps.placement_states
            .lock()
            .await
            .insert(fragment, FragmentPlacementState::Bootstrapping);

        let stale = vec!["node-a".to_string(), "node-b".to_string()];
        assert_eq!(
            commit_fragment_gain(&deps, fragment, &stale, 0).await,
            FragmentGainCommit::Retry
        );
        assert!(!serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Bootstrapping)
        );

        assert_eq!(
            commit_fragment_gain(&deps, fragment, &current, 0).await,
            FragmentGainCommit::Serving
        );
        assert!(serving.read().unwrap().contains(&fragment));
    }

    #[tokio::test]
    async fn completed_bootstrap_retries_after_placement_aba() {
        let fragment = FragmentId(0);
        let serving = Arc::new(RwLock::new(HashSet::new()));
        let replicas = vec!["node-a".to_string(), "node-b".to_string()];
        let placement = Arc::new(RwLock::new(HashMap::from([(fragment, replicas.clone())])));
        let deps = test_placement_deps(
            FragmentRouter::new(1, 0),
            serving.clone(),
            placement,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );
        deps.placement_generation.store(2, Ordering::Release);

        assert_eq!(
            commit_fragment_gain(&deps, fragment, &replicas, 0).await,
            FragmentGainCommit::Retry
        );
        assert!(!serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Bootstrapping)
        );
    }

    #[tokio::test]
    async fn stopped_sender_entry_is_replaced_in_snapshot_recovery() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let config = NodeConfig::default();
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("replace-sender-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peer_failures = PeerFailureSignals::default();
        let finalizing_senders = Arc::new(SenderFinalizationRegistry::default());
        let retiring_senders = Arc::new(RetiringSenderRegistry::default());
        let sender_config = SenderConfig::default();
        let runtime = SenderRuntimeOptions::default();
        let operation_gate = OperationGate::new();

        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
            },
        )
        .await;
        let original = senders
            .lock()
            .await
            .get(&(fragment, "peer".to_string()))
            .cloned()
            .unwrap();
        original.stop().await;

        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
            },
        )
        .await;
        let replacement = senders
            .lock()
            .await
            .get(&(fragment, "peer".to_string()))
            .cloned()
            .unwrap();
        assert!(!replacement.is_closed());
        assert!(!replacement.ingress_is_idle());
        assert_eq!(reports.lock().await.len(), 1);

        replacement.stop().await;
        router.remove(fragment);
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn stopped_drain_sender_returns_to_retryable_preparation() {
        let fragment = FragmentId(0);
        let node = Node::start(
            "drain-replacement-source".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            NodeConfig {
                fragment_map: HashMap::from([(
                    fragment,
                    vec![
                        "drain-replacement-source".to_string(),
                        "absent-target".to_string(),
                    ],
                )]),
                ..NodeConfig::default()
            },
        )
        .await
        .unwrap();
        node.put("replacement-fence".to_string(), b"value".to_vec())
            .await
            .unwrap();
        assert!(matches!(
            node.shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1))
            )
            .await,
            Err(ReplicationDrainShutdownError::Timeout {
                phase: ReplicationDrainPhase::PeerReconciliation,
                ..
            })
        ));

        let stopped = node
            .senders
            .lock()
            .await
            .get(&(fragment, "absent-target".to_string()))
            .cloned()
            .unwrap();
        stopped.stop().await;
        assert!(matches!(
            node.shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1))
            )
            .await,
            Err(ReplicationDrainShutdownError::RetryableSender(_))
        ));

        // The failed exact waiter moved the coordinator back to preparation.
        // A retry replaces the dead task and waits on a new generation rather
        // than returning the same permanent Unavailable error.
        assert!(matches!(
            node.shutdown_with_replication_drain(
                ReplicationDrainOptions::default().with_timeout(Duration::from_secs(1))
            )
            .await,
            Err(ReplicationDrainShutdownError::Timeout {
                phase: ReplicationDrainPhase::PeerReconciliation,
                ..
            })
        ));
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn drain_set_ignores_other_fragments_and_requires_a_matching_report() {
        let target = FragmentId(0);
        let other = FragmentId(1);
        let other_report = test_sender_report();
        let other_sender = spawn_node_sender(
            other,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(2, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: other_report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let other_key = (other, "peer".to_string());
        let mut senders = HashMap::from([(other_key.clone(), other_sender.clone())]);
        let reports = HashMap::from([(other_key, other_report)]);
        assert!(fragment_sender_set_is_drained(target, &senders, &reports).await);

        let target_sender = spawn_node_sender(
            target,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(2, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: test_sender_report(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        senders.insert((target, "peer".to_string()), target_sender.clone());
        assert!(!fragment_sender_set_is_drained(target, &senders, &reports).await);

        target_sender.stop().await;
        other_sender.stop().await;
    }

    #[tokio::test]
    async fn report_wait_releases_global_registries_and_restores_a_readded_fragment() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let config = NodeConfig::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("readd-at-commit-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let supervisors = Arc::new(Mutex::new(HashMap::from([(fragment, supervisor.clone())])));
        let serving = Arc::new(RwLock::new(HashSet::from([fragment])));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["peer".to_string()],
        )])));
        let report = test_sender_report();
        let report_guard = report.lock().await;
        let sender = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: router.clone(),
                hlc: hlc.clone(),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let key = (fragment, "peer".to_string());
        let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
        let reports = Arc::new(Mutex::new(HashMap::from([(key, report.clone())])));
        let deps = test_placement_deps(
            router.clone(),
            serving.clone(),
            placement.clone(),
            supervisors.clone(),
            senders.clone(),
            reports,
        );
        let loss_deps = deps.clone();
        let loss = tokio::spawn(async move {
            lose_fragment(&loss_deps, fragment).await;
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while serving.read().unwrap().contains(&fragment) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("loss transition did not revoke serving");
        assert!(
            !loss.is_finished(),
            "locked sender report must delay teardown"
        );
        let senders_guard = tokio::time::timeout(Duration::from_millis(100), senders.lock())
            .await
            .expect("sender registry stayed locked across a report wait");
        drop(senders_guard);
        let reports_guard =
            tokio::time::timeout(Duration::from_millis(100), deps.sender_reports.lock())
                .await
                .expect("report registry stayed locked across an entry report wait");
        drop(reports_guard);
        {
            let _states = deps.placement_states.lock().await;
            placement
                .write()
                .unwrap()
                .insert(fragment, vec!["node-a".to_string()]);
        }
        drop(report_guard);
        tokio::time::timeout(Duration::from_secs(1), loss)
            .await
            .expect("re-added fragment did not leave teardown")
            .unwrap();

        assert!(serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Serving)
        );
        assert!(router.route_for_fragment(fragment).is_some());
        assert!(supervisors.lock().await.contains_key(&fragment));
        sender.stop().await;
        router.remove(fragment);
        supervisors.lock().await.remove(&fragment);
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn final_teardown_waits_on_only_the_fragment_mutation_fence() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let config = NodeConfig::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("sender-change-at-commit-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let supervisors = Arc::new(Mutex::new(HashMap::from([(fragment, supervisor.clone())])));
        let mutation_gate = router.mutation_gate(fragment);
        let serving = Arc::new(RwLock::new(HashSet::from([fragment])));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["peer".to_string()],
        )])));
        let report = test_sender_report();
        let report_guard = report.lock().await;
        let sender = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: router.clone(),
                hlc: hlc.clone(),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let key = (fragment, "peer".to_string());
        let senders = Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
        let reports = Arc::new(Mutex::new(HashMap::from([(key, report.clone())])));
        let deps = test_placement_deps(
            router.clone(),
            serving.clone(),
            placement.clone(),
            supervisors.clone(),
            senders.clone(),
            reports,
        );
        let loss_deps = deps.clone();
        let loss = tokio::spawn(async move {
            lose_fragment(&loss_deps, fragment).await;
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while serving.read().unwrap().contains(&fragment) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("loss transition did not revoke serving");
        let admitted = mutation_gate.read_owned().await;
        drop(report_guard);
        let publish_states = deps.placement_states.clone();
        let publish_placement = placement.clone();
        let publisher = tokio::spawn(async move {
            let _states = publish_states.lock().await;
            publish_placement
                .write()
                .unwrap()
                .insert(fragment, vec!["node-a".to_string()]);
        });
        tokio::time::timeout(Duration::from_secs(1), publisher)
            .await
            .expect("placement publication was blocked by a fragment mutation fence")
            .unwrap();
        drop(admitted);
        tokio::time::timeout(Duration::from_secs(1), loss)
            .await
            .expect("teardown did not restore the re-added fragment")
            .unwrap();

        assert!(serving.read().unwrap().contains(&fragment));
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Serving)
        );
        assert!(!senders
            .lock()
            .await
            .contains_key(&(fragment, "peer".to_string())));
        assert!(router.route_for_fragment(fragment).is_some());
        assert!(supervisors.lock().await.contains_key(&fragment));
        sender.stop().await;
        router.remove(fragment);
        supervisors.lock().await.remove(&fragment);
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn teardown_without_registered_supervisor_stays_draining() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let config = NodeConfig::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("teardown-supervisor-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let serving = Arc::new(RwLock::new(HashSet::from([fragment])));
        let placement = Arc::new(RwLock::new(HashMap::from([(
            fragment,
            vec!["peer".to_string()],
        )])));
        let supervisors = Arc::new(Mutex::new(HashMap::new()));
        let deps = test_placement_deps(
            router.clone(),
            serving.clone(),
            placement,
            supervisors.clone(),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(100), lose_fragment(&deps, fragment))
                .await
                .is_err(),
            "teardown claimed completion without lifecycle ownership"
        );
        assert!(!serving.read().unwrap().contains(&fragment));
        assert!(router.route_for_fragment(fragment).is_some());
        assert!(supervisors.lock().await.is_empty());
        assert_eq!(
            deps.placement_states.lock().await.get(&fragment),
            Some(&FragmentPlacementState::Draining)
        );
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn stale_sender_ensure_refuses_a_removed_router_fragment() {
        let router = FragmentRouter::new(1, 0);
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let peer_failures = PeerFailureSignals::default();
        let finalizing_senders = Arc::new(SenderFinalizationRegistry::default());
        let retiring_senders = Arc::new(RetiringSenderRegistry::default());
        let config = SenderConfig::default();
        let runtime = SenderRuntimeOptions::default();
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let operation_gate = OperationGate::new();

        ensure_sender_entry(
            FragmentId(0),
            "peer",
            SenderEntryDeps {
                config: &config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
            },
        )
        .await;

        assert!(senders.lock().await.is_empty());
        assert!(reports.lock().await.is_empty());
    }

    #[tokio::test]
    async fn finalizing_fragment_refuses_sender_recreation_until_the_seal_drops() {
        let fragment = FragmentId(0);
        let router = FragmentRouter::new(1, 0);
        let hlc = Arc::new(Mutex::new(HlcClock::new()));
        let config = NodeConfig::default();
        let (supervisor, dispatcher) =
            spawn_fragment_subtree("sealed-sender-test", fragment, &config, &hlc)
                .await
                .unwrap();
        router.insert(fragment, dispatcher);
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::new()));
        let finalizing_senders = Arc::new(SenderFinalizationRegistry::default());
        let retiring_senders = Arc::new(RetiringSenderRegistry::default());
        let peer_failures = PeerFailureSignals::default();
        let sender_config = SenderConfig::default();
        let runtime = SenderRuntimeOptions::default();
        let operation_gate = OperationGate::new();
        let seal = {
            let _registry_order = senders.lock().await;
            finalizing_senders.seal(fragment).unwrap()
        };

        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
            },
        )
        .await;
        assert!(senders.lock().await.is_empty());
        assert!(reports.lock().await.is_empty());

        drop(seal);
        ensure_sender_entry(
            fragment,
            "peer",
            SenderEntryDeps {
                config: &sender_config,
                runtime: &runtime,
                router: &router,
                hlc: &hlc,
                senders: &senders,
                finalizing_senders: &finalizing_senders,
                retiring_senders: &retiring_senders,
                reports: &reports,
                peer_failures: &peer_failures,
                operation_gate: &operation_gate,
                allow_quiescing: false,
            },
        )
        .await;
        let sender = senders
            .lock()
            .await
            .get(&(fragment, "peer".to_string()))
            .cloned()
            .expect("sender was not recreated after finalization ended");

        sender.stop().await;
        router.remove(fragment);
        let _ = supervisor.stop_and_wait(None, None).await;
    }

    #[tokio::test]
    async fn sender_snapshot_identity_rejects_same_key_replacement() {
        let fragment = FragmentId(0);
        let key = (fragment, "peer".to_string());
        let report = test_sender_report();
        let sender = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let replacement_report = test_sender_report();
        let replacement = spawn_node_sender(
            fragment,
            "peer".to_string(),
            SenderConfig::default(),
            SenderDeps {
                router: FragmentRouter::new(1, 0),
                hlc: Arc::new(Mutex::new(HlcClock::new())),
                state_report: replacement_report.clone(),
            },
            SenderRuntimeOptions::default(),
        )
        .unwrap();
        let senders: SenderMap =
            Arc::new(Mutex::new(HashMap::from([(key.clone(), sender.clone())])));
        let reports: SenderReportMap = Arc::new(Mutex::new(HashMap::from([(key.clone(), report)])));
        assert!(sender_task_is_current(&senders, &key, &sender).await);
        let snapshot = {
            let senders = senders.lock().await;
            let reports = reports.lock().await;
            snapshot_fragment_sender_set(fragment, &senders, &reports).unwrap()
        };

        senders
            .lock()
            .await
            .insert(key.clone(), replacement.clone());
        reports.lock().await.insert(key.clone(), replacement_report);
        assert!(!sender_task_is_current(&senders, &key, &sender).await);
        let drained = lock_drained_fragment_sender_snapshot(snapshot)
            .await
            .expect("detached empty sender should be drainable");
        {
            let senders = senders.lock().await;
            let reports = reports.lock().await;
            assert!(!fragment_sender_snapshot_is_current(
                fragment,
                &drained.entries,
                &senders,
                &reports,
            ));
        }
        drop(drained);

        sender.stop().await;
        replacement.stop().await;
    }

    fn test_placement_deps(
        router: FragmentRouter,
        serving: Arc<RwLock<HashSet<FragmentId>>>,
        placement: Arc<RwLock<HashMap<FragmentId, Vec<String>>>>,
        supervisors: Arc<Mutex<HashMap<FragmentId, ActorRef<SupervisorMessage>>>>,
        senders: SenderMap,
        sender_reports: SenderReportMap,
    ) -> PlacementDriverDeps {
        let supervisor_stops = Arc::new(SupervisorRegistry::new(1));
        if let Ok(active) = supervisors.try_lock() {
            for (fragment, supervisor) in active.iter() {
                supervisor_stops.insert_ready(*fragment, supervisor.clone());
            }
        }
        PlacementDriverDeps {
            expiry_publisher: None,
            recovery: None,
            node_id: "node-a".to_string(),
            controller: Arc::new(crate::placement::StaticPlacement::new(HashMap::new())),
            poll_interval: Duration::from_secs(3600),
            drain_warn_interval: Duration::from_secs(3600),
            fragment_count: 1,
            subtree_config: NodeConfig::default(),
            router,
            serving,
            placement,
            supervisors,
            supervisor_stops,
            placement_states: Arc::new(Mutex::new(HashMap::from([(
                FragmentId(0),
                FragmentPlacementState::Draining,
            )]))),
            preloaded_fragments: Arc::new(StdMutex::new(HashSet::new())),
            placement_generation: Arc::new(AtomicU64::new(0)),
            senders,
            finalizing_senders: Arc::new(SenderFinalizationRegistry::default()),
            retiring_senders: Arc::new(RetiringSenderRegistry::default()),
            sender_reports,
            sender_config: SenderConfig::default(),
            sender_runtime: SenderRuntimeOptions::default(),
            peers: Arc::new(Mutex::new(HashMap::new())),
            peer_failures: PeerFailureSignals::default(),
            tasks: Weak::new(),
            dispatcher_tasks: Weak::new(),
            operation_gate: OperationGate::new(),
            hlc: Arc::new(Mutex::new(HlcClock::new())),
        }
    }
}
