// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Authenticated process generation authority and hot receiver fencing.
//!
//! Credentials and generation allocation belong to the application. A fence
//! retires this receiver's sessions and joins their actual owned work; it does
//! not establish consensus, revoke credentials at other receivers, or elect a
//! writer. Historical record writer IDs remain independent of session trust.

use super::*;
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};

/// Public-key tombstones are retained for the receiver's whole lifetime.
/// Exhaustion rejects new keys before closing admission; no entry is evicted.
pub const MAX_HISTORICAL_PEER_KEYS: usize = 65_536;

/// One externally allocated process generation and its independent random key.
#[derive(Clone, PartialEq, Eq)]
pub struct PeerCredential {
    generation: u64,
    signing_seed: [u8; 32],
}
impl std::fmt::Debug for PeerCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerCredential")
            .field("generation", &self.generation)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
impl PeerCredential {
    /// Supply 32 independently generated random seed bytes. Generation
    /// must be nonzero; replacing a process requires a fresh key and generation.
    pub fn new(generation: u64, signing_seed: [u8; 32]) -> Self {
        Self {
            generation,
            signing_seed,
        }
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Distribute only this public identity to verifying peers. Never share
    /// the process signing seed with another process or retired instance.
    pub fn public_identity(&self) -> PeerIdentity {
        let pair =
            Ed25519KeyPair::from_seed_unchecked(&self.signing_seed).expect("32-byte Ed25519 seed");
        PeerIdentity {
            generation: self.generation,
            public_key: pair
                .public_key()
                .as_ref()
                .try_into()
                .expect("32-byte public key"),
        }
    }
    pub(super) fn sign(&self, prover_is_dialer: bool, dialer: &Hello, listener: &Hello) -> Vec<u8> {
        Ed25519KeyPair::from_seed_unchecked(&self.signing_seed)
            .expect("32-byte Ed25519 seed")
            .sign(&proof_bytes(prover_is_dialer, dialer, listener))
            .as_ref()
            .to_vec()
    }
}

/// Expected public identity of one externally authorized process generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerIdentity {
    generation: u64,
    public_key: [u8; 32],
}
impl PeerIdentity {
    pub fn new(generation: u64, public_key: [u8; 32]) -> Self {
        Self {
            generation,
            public_key,
        }
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn public_key(&self) -> &[u8; 32] {
        &self.public_key
    }
    pub(super) fn verify(
        &self,
        prover_is_dialer: bool,
        dialer: &Hello,
        listener: &Hello,
        proof: &[u8],
    ) -> bool {
        UnparsedPublicKey::new(&ED25519, &self.public_key)
            .verify(&proof_bytes(prover_is_dialer, dialer, listener), proof)
            .is_ok()
    }
}
fn proof_bytes(prover_is_dialer: bool, dialer: &Hello, listener: &Hello) -> Vec<u8> {
    let mut bytes = b"actordb-process-generation-signature-v1\0".to_vec();
    bytes.extend_from_slice(if prover_is_dialer {
        b"dialer"
    } else {
        b"listener"
    });
    bytes.extend_from_slice(&super::connection::handshake_transcript(dialer, listener));
    bytes
}

/// Local proof key and exact authorized remote identities/process keys.
/// All keys must be distinct. Sharing keys would allow one process to claim
/// another identity, so it is rejected. Debug output never includes signing seeds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerAuthorityOptions {
    local: PeerCredential,
    peers: HashMap<String, PeerIdentity>,
}
impl PeerAuthorityOptions {
    pub fn new(local: PeerCredential, peers: HashMap<String, PeerIdentity>) -> Self {
        Self { local, peers }
    }
    pub fn local_generation(&self) -> u64 {
        self.local.generation
    }
    pub fn local_public_identity(&self) -> PeerIdentity {
        self.local.public_identity()
    }
    /// Expected public identities; contains no peer signing secrets.
    pub fn peer_identities(&self) -> &HashMap<String, PeerIdentity> {
        &self.peers
    }
    pub fn with_peer(mut self, id: impl Into<String>, identity: PeerIdentity) -> Self {
        self.peers.insert(id.into(), identity);
        self
    }
    pub fn without_peer(mut self, id: &str) -> Self {
        self.peers.remove(id);
        self
    }
    pub fn with_local_credential(mut self, local: PeerCredential) -> Self {
        self.local = local;
        self
    }
    /// Exact expected peer generations. Returned values contain no credentials.
    pub fn peer_generations(&self) -> Vec<AuthorizedPeerGeneration> {
        let mut peers: Vec<_> = self
            .peers
            .iter()
            .map(|(id, credential)| AuthorizedPeerGeneration {
                peer_id: id.clone(),
                generation: credential.generation,
            })
            .collect();
        peers.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        peers
    }
    pub(super) fn validate(&self) -> Result<(), String> {
        if self.peers.len() > 1024 {
            return Err("peer authority supports at most 1024 authorized peers".into());
        }
        let mut keys = HashSet::new();
        let local = self.local.public_identity();
        for identity in std::iter::once(&local).chain(self.peers.values()) {
            if identity.generation == 0 || identity.public_key.iter().all(|byte| *byte == 0) {
                return Err(
                    "peer identities require nonzero generations and valid 32-byte public keys"
                        .into(),
                );
            }
            if !keys.insert(&identity.public_key) {
                return Err("each process must have an independent peer signing key".into());
            }
        }
        if self
            .peers
            .keys()
            .any(|id| id.is_empty() || id.len() > MAX_WIRE_NODE_ID_BYTES)
        {
            return Err("authorized peer IDs must contain 1 to 255 bytes".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizedPeerGeneration {
    peer_id: String,
    generation: u64,
}
impl AuthorizedPeerGeneration {
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerFencePhase {
    Unconfigured,
    Ready,
    Fencing,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerFenceStage {
    WaitingOldFrames,
    WaitingPersistence,
    FlushingStore,
    Blocked,
}

/// Public cryptographic identity of this receiver's exact configured cohort.
/// Keys here verify signatures; they do not grant signing authority. A local
/// revision is scoped to one receiver process, not an external membership epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerAuthoritySnapshot {
    receiver_id: String,
    revision: u64,
    phase: PeerFencePhase,
    local: PeerIdentity,
    peers: HashMap<String, PeerIdentity>,
}
impl PeerAuthoritySnapshot {
    pub fn receiver_id(&self) -> &str {
        &self.receiver_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn phase(&self) -> PeerFencePhase {
        self.phase
    }
    pub fn local_public_identity(&self) -> &PeerIdentity {
        &self.local
    }
    pub fn peer_identities(&self) -> &HashMap<String, PeerIdentity> {
        &self.peers
    }
}
struct FenceProgress {
    stage: PeerFenceStage,
    error: Option<String>,
}

/// Bounded local snapshot, requiring no actor or store RPC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerFenceStatus {
    phase: PeerFencePhase,
    revision: u64,
    pending_revision: Option<u64>,
    local_generation: u64,
    pending_local_generation: Option<u64>,
    outstanding_frames: usize,
    outstanding_persistence: usize,
    authorized_peers: Vec<AuthorizedPeerGeneration>,
    pending_authorized_peers: Option<Vec<AuthorizedPeerGeneration>>,
    missing_peers: Vec<String>,
    retired_sessions: usize,
    stage: Option<PeerFenceStage>,
    blocking_error: Option<String>,
    accepted_frames: u64,
    completed_frames: u64,
    rejected_frames: u64,
    rejected_handshakes: u64,
    historical_key_count: usize,
}
impl PeerFenceStatus {
    pub fn phase(&self) -> PeerFencePhase {
        self.phase
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn pending_revision(&self) -> Option<u64> {
        self.pending_revision
    }
    pub fn local_generation(&self) -> u64 {
        self.local_generation
    }
    pub fn pending_local_generation(&self) -> Option<u64> {
        self.pending_local_generation
    }
    pub fn outstanding_frames(&self) -> usize {
        self.outstanding_frames
    }
    pub fn outstanding_persistence(&self) -> usize {
        self.outstanding_persistence
    }
    pub fn authorized_peers(&self) -> &[AuthorizedPeerGeneration] {
        &self.authorized_peers
    }
    pub fn pending_authorized_peers(&self) -> Option<&[AuthorizedPeerGeneration]> {
        self.pending_authorized_peers.as_deref()
    }
    /// Configured peers without a current eligible live session. Empty in
    /// legacy mode, where expected membership is not configured by this API.
    pub fn missing_peers(&self) -> &[String] {
        &self.missing_peers
    }
    /// Old registered sessions still releasing ownership during a transition.
    pub fn retired_sessions(&self) -> usize {
        self.retired_sessions
    }
    pub fn stage(&self) -> Option<PeerFenceStage> {
        self.stage
    }
    pub fn blocking_error(&self) -> Option<&str> {
        self.blocking_error.as_deref()
    }
    /// Cumulative admitted decoded frames in this receiver process.
    pub fn accepted_frames(&self) -> u64 {
        self.accepted_frames
    }
    /// Terminal ownership release, including failed or rejected work.
    pub fn completed_frames(&self) -> u64 {
        self.completed_frames
    }
    pub fn rejected_frames(&self) -> u64 {
        self.rejected_frames
    }
    pub fn rejected_handshakes(&self) -> u64 {
        self.rejected_handshakes
    }
    pub fn historical_key_count(&self) -> usize {
        self.historical_key_count
    }
    pub fn historical_key_limit(&self) -> usize {
        MAX_HISTORICAL_PEER_KEYS
    }
}

#[derive(Clone, Debug, bon::Builder)]
pub struct PeerFenceOptions {
    #[builder(default = Duration::from_secs(30))]
    timeout: Duration,
}
impl Default for PeerFenceOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}
impl PeerFenceOptions {
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Evidence that this receiver retired every old authority session, joined
/// accepted frame/actor work and reached its configured local store fence.
/// It does not certify remote replicas, exact cache equality, or a writer lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerFenceReport {
    previous_revision: u64,
    revision: u64,
    local_generation: u64,
    completed_frames: u64,
    local_durability: ApplyDurability,
    authorized_peers: Vec<AuthorizedPeerGeneration>,
    public_authority: PeerAuthoritySnapshot,
}
impl PeerFenceReport {
    pub fn previous_revision(&self) -> u64 {
        self.previous_revision
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn local_generation(&self) -> u64 {
        self.local_generation
    }
    pub fn completed_frames(&self) -> u64 {
        self.completed_frames
    }
    pub fn local_durability(&self) -> ApplyDurability {
        self.local_durability
    }
    pub fn authorized_peers(&self) -> &[AuthorizedPeerGeneration] {
        &self.authorized_peers
    }
    /// Exact completed policy, including public keys for external manifest
    /// qualification. Check current Node authority separately before activation.
    pub fn public_authority(&self) -> &PeerAuthoritySnapshot {
        &self.public_authority
    }
}
#[derive(Debug)]
#[non_exhaustive]
pub enum PeerFenceError {
    InvalidOptions(String),
    DifferentPendingAuthority,
    Timeout { status: Box<PeerFenceStatus> },
    Local(NodeError),
    ShuttingDown,
}
impl std::fmt::Display for PeerFenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidOptions(error) => write!(f, "invalid peer authority: {error}"),
            Self::DifferentPendingAuthority => {
                f.write_str("retry must provide the exact pending peer authority")
            }
            Self::Timeout { status } => write!(
                f,
                "peer fence timed out: {} old frames and {} persistence operations remain",
                status.outstanding_frames, status.outstanding_persistence
            ),
            Self::Local(error) => write!(f, "peer fence local persistence failed: {error}"),
            Self::ShuttingDown => f.write_str("peer authority is permanently closed for shutdown"),
        }
    }
}
impl std::error::Error for PeerFenceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Local(error) => Some(error),
            _ => None,
        }
    }
}

pub(super) struct AuthorityEra {
    revision: u64,
    options: Option<PeerAuthorityOptions>,
    gate: Arc<OperationGate>,
    connections: StdMutex<HashMap<u64, (String, PeerChannel)>>,
    next_connection: AtomicU64,
}
impl AuthorityEra {
    fn new(revision: u64, options: Option<PeerAuthorityOptions>) -> Arc<Self> {
        Arc::new(Self {
            revision,
            options,
            gate: OperationGate::new(),
            connections: StdMutex::new(HashMap::new()),
            next_connection: AtomicU64::new(0),
        })
    }
    pub(super) fn local_generation(&self) -> u64 {
        self.options
            .as_ref()
            .map_or(0, |options| options.local.generation)
    }
    fn public_snapshot(
        &self,
        receiver_id: &str,
        phase: PeerFencePhase,
    ) -> Option<PeerAuthoritySnapshot> {
        self.options.as_ref().map(|options| PeerAuthoritySnapshot {
            receiver_id: receiver_id.to_string(),
            revision: self.revision,
            phase,
            local: options.local.public_identity(),
            peers: options.peers.clone(),
        })
    }
    fn expected_identity(&self, peer: &Hello) -> Result<Option<&PeerIdentity>, ()> {
        match &self.options {
            None if peer.process_generation == 0 => Ok(None),
            None => Err(()),
            Some(options) => options
                .peers
                .get(&peer.node_id)
                .filter(|credential| {
                    credential.generation == peer.process_generation
                        && peer.capabilities & CAPABILITY_AUTHENTICATED_PEER_GENERATIONS != 0
                })
                .map(Some)
                .ok_or(()),
        }
    }
    pub(super) fn accepts(&self, peer: &Hello) -> bool {
        self.expected_identity(peer).is_ok()
    }
    pub(super) fn sign(
        &self,
        legacy_secret: Option<&[u8]>,
        role: bool,
        dialer: &Hello,
        listener: &Hello,
    ) -> Vec<u8> {
        match &self.options {
            Some(options) => options.local.sign(role, dialer, listener),
            None => legacy_secret
                .map(|key| super::connection::handshake_auth_tag(key, role, dialer, listener))
                .unwrap_or_default(),
        }
    }
    pub(super) fn verify(
        &self,
        legacy_secret: Option<&[u8]>,
        role: bool,
        dialer: &Hello,
        listener: &Hello,
        proof: &[u8],
    ) -> bool {
        let peer = if role { dialer } else { listener };
        match self.expected_identity(peer) {
            Ok(Some(identity)) => identity.verify(role, dialer, listener, proof),
            Ok(None) => super::connection::verify_handshake_auth(
                legacy_secret,
                role,
                dialer,
                listener,
                proof,
            ),
            Err(_) => false,
        }
    }
    fn cancel_connections(&self) {
        self.gate.begin_shutdown();
        let channels: Vec<_> = self
            .connections
            .lock()
            .unwrap()
            .values()
            .map(|(_, channel)| channel.clone())
            .collect();
        for channel in channels {
            channel.cancel();
        }
    }
}

pub(super) struct PeerSession {
    era: Arc<AuthorityEra>,
    connection: u64,
}
impl Drop for PeerSession {
    fn drop(&mut self) {
        self.era
            .connections
            .lock()
            .unwrap()
            .remove(&self.connection);
    }
}
struct PendingFence {
    old: Arc<AuthorityEra>,
    next: PeerAuthorityOptions,
    persistence: Arc<OperationGate>,
    progress: StdMutex<FenceProgress>,
}
struct AuthorityState {
    era: Arc<AuthorityEra>,
    pending: Option<Arc<PendingFence>>,
    stopped: bool,
    high_water: HashMap<String, PeerIdentity>,
    seen_public_keys: HashSet<[u8; 32]>,
    last_report: Option<PeerFenceReport>,
}
pub(super) struct PeerAuthority {
    state: StdMutex<AuthorityState>,
    coordinator: Mutex<()>,
    node_id: String,
    accepted_frames: AtomicU64,
    completed_frames: Arc<AtomicU64>,
    rejected_frames: AtomicU64,
    rejected_handshakes: AtomicU64,
}
struct PeerWork {
    _permit: OperationPermit,
    completed: Arc<AtomicU64>,
}
impl Drop for PeerWork {
    fn drop(&mut self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}
impl PeerAuthority {
    pub(super) fn new(node_id: String, options: Option<PeerAuthorityOptions>) -> Arc<Self> {
        let high_water = options.as_ref().map_or_else(HashMap::new, |options| {
            let mut peers = options.peers.clone();
            peers.insert(node_id.clone(), options.local.public_identity());
            peers
        });
        let seen_public_keys = high_water
            .values()
            .map(|identity| identity.public_key)
            .collect();
        Arc::new(Self {
            state: StdMutex::new(AuthorityState {
                era: AuthorityEra::new(1, options),
                pending: None,
                stopped: false,
                high_water,
                seen_public_keys,
                last_report: None,
            }),
            coordinator: Mutex::new(()),
            node_id,
            accepted_frames: AtomicU64::new(0),
            completed_frames: Arc::new(AtomicU64::new(0)),
            rejected_frames: AtomicU64::new(0),
            rejected_handshakes: AtomicU64::new(0),
        })
    }
    pub(super) fn handshake_era(&self) -> Option<Arc<AuthorityEra>> {
        let state = self.state.lock().unwrap();
        if state.stopped || state.pending.is_some() {
            self.reject_handshake();
        }
        (!state.stopped && state.pending.is_none()).then(|| state.era.clone())
    }
    pub(super) fn reject_handshake(&self) {
        self.rejected_handshakes.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn register(
        &self,
        era: &Arc<AuthorityEra>,
        peer: &str,
        channel: PeerChannel,
    ) -> Option<PeerSession> {
        let state = self.state.lock().unwrap();
        if state.stopped || state.pending.is_some() || !Arc::ptr_eq(era, &state.era) {
            self.reject_handshake();
            return None;
        }
        let connection = era.next_connection.fetch_add(1, Ordering::Relaxed);
        era.connections
            .lock()
            .unwrap()
            .insert(connection, (peer.to_string(), channel));
        Some(PeerSession {
            era: era.clone(),
            connection,
        })
    }
    pub(super) fn admit(&self, session: &PeerSession) -> Result<Arc<dyn Send + Sync>, NodeError> {
        let state = self.state.lock().unwrap();
        if state.stopped || state.pending.is_some() || !Arc::ptr_eq(&session.era, &state.era) {
            self.rejected_frames.fetch_add(1, Ordering::Relaxed);
            return Err(NodeError::ShuttingDown);
        }
        let permit = session.era.gate.try_enter()?;
        self.accepted_frames.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(PeerWork {
            _permit: permit,
            completed: self.completed_frames.clone(),
        }))
    }
    pub(super) fn close(&self) {
        let era = {
            let mut state = self.state.lock().unwrap();
            state.stopped = true;
            if let Some(pending) = &state.pending {
                pending.persistence.begin_shutdown();
            }
            state.era.clone()
        };
        era.cancel_connections();
    }
    pub(super) async fn wait_closed(&self) {
        let (era, pending) = {
            let state = self.state.lock().unwrap();
            (state.era.clone(), state.pending.clone())
        };
        era.gate.wait_for_drain().await;
        if let Some(pending) = pending {
            pending.persistence.wait_for_drain().await;
        }
    }
    fn begin(&self, next: PeerAuthorityOptions) -> Result<(), PeerFenceError> {
        next.validate().map_err(PeerFenceError::InvalidOptions)?;
        if next.peers.contains_key(&self.node_id) {
            return Err(PeerFenceError::InvalidOptions(
                "local identity must not appear in authorized remote peers".into(),
            ));
        }
        let old = {
            let mut state = self.state.lock().unwrap();
            if state.stopped {
                return Err(PeerFenceError::ShuttingDown);
            }
            if let Some(pending) = &state.pending {
                return if pending.next == next {
                    Ok(())
                } else {
                    Err(PeerFenceError::DifferentPendingAuthority)
                };
            }
            if state.era.options.as_ref() == Some(&next) {
                return if state.last_report.is_some() {
                    Ok(())
                } else {
                    Err(PeerFenceError::InvalidOptions(
                        "authority is unchanged; no process or session was revoked".into(),
                    ))
                };
            }
            if state.era.revision == u64::MAX {
                return Err(PeerFenceError::InvalidOptions(
                    "authority revision exhausted".into(),
                ));
            }
            let new_ids = next
                .peers
                .keys()
                .filter(|id| !state.high_water.contains_key(*id))
                .count();
            if state.high_water.len().saturating_add(new_ids) > 65_536 {
                return Err(PeerFenceError::InvalidOptions(
                    "peer authority historical identity limit (65536) exhausted".into(),
                ));
            }
            let local = next.local.public_identity();
            let new_keys = std::iter::once(&local)
                .chain(next.peers.values())
                .filter(|identity| !state.seen_public_keys.contains(&identity.public_key))
                .count();
            if state.seen_public_keys.len().saturating_add(new_keys) > MAX_HISTORICAL_PEER_KEYS {
                return Err(PeerFenceError::InvalidOptions("peer authority historical public-key limit exhausted; key tombstones cannot be evicted".into()));
            }
            for (id, credential) in
                std::iter::once((&self.node_id, &local)).chain(next.peers.iter())
            {
                if let Some(previous) = state.high_water.get(id) {
                    let still_present = id == &self.node_id
                        || state
                            .era
                            .options
                            .as_ref()
                            .is_some_and(|options| options.peers.contains_key(id));
                    if credential == previous && still_present {
                        continue;
                    }
                    if credential.generation <= previous.generation {
                        return Err(PeerFenceError::InvalidOptions("replaced or readded processes require a newer generation and a fresh independent key".into()));
                    }
                }
                // Any historical key, even one retired several rotations ago
                // or formerly owned by another ID, grants its old signer the
                // ability to impersonate this new generation. Only the exact
                // unchanged current ID/generation exception above is allowed.
                if state.seen_public_keys.contains(&credential.public_key) {
                    return Err(PeerFenceError::InvalidOptions(
                        "a historically admitted peer signing key cannot be reused or reassigned"
                            .into(),
                    ));
                }
            }
            let old = state.era.clone();
            old.gate.begin_shutdown();
            state.pending = Some(Arc::new(PendingFence {
                old: old.clone(),
                next,
                persistence: OperationGate::new(),
                progress: StdMutex::new(FenceProgress {
                    stage: PeerFenceStage::WaitingOldFrames,
                    error: None,
                }),
            }));
            old
        };
        old.cancel_connections();
        Ok(())
    }
    fn status(&self) -> PeerFenceStatus {
        let state = self.state.lock().unwrap();
        let connections = state.era.connections.lock().unwrap();
        let eligible: HashSet<_> = connections
            .values()
            .filter(|(_, channel)| {
                !state.stopped && state.pending.is_none() && !*channel.cancel.borrow()
            })
            .map(|(id, _)| id.as_str())
            .collect();
        let mut missing_peers: Vec<_> =
            state.era.options.as_ref().map_or_else(Vec::new, |options| {
                options
                    .peers
                    .keys()
                    .filter(|id| !eligible.contains(id.as_str()))
                    .cloned()
                    .collect()
            });
        missing_peers.sort();
        let progress = state.pending.as_ref().map(|pending| {
            let progress = pending.progress.lock().unwrap();
            (progress.stage, progress.error.clone())
        });
        PeerFenceStatus {
            phase: if state.stopped {
                PeerFencePhase::Stopped
            } else if state.pending.is_some() {
                PeerFencePhase::Fencing
            } else if state.era.options.is_some() {
                PeerFencePhase::Ready
            } else {
                PeerFencePhase::Unconfigured
            },
            revision: state.era.revision,
            pending_revision: state
                .pending
                .as_ref()
                .map(|pending| pending.old.revision + 1),
            local_generation: state.era.local_generation(),
            pending_local_generation: state
                .pending
                .as_ref()
                .map(|pending| pending.next.local_generation()),
            outstanding_frames: state.era.gate.active.load(Ordering::SeqCst),
            outstanding_persistence: state.pending.as_ref().map_or(0, |pending| {
                pending.persistence.active.load(Ordering::SeqCst)
            }),
            authorized_peers: state
                .era
                .options
                .as_ref()
                .map_or_else(Vec::new, PeerAuthorityOptions::peer_generations),
            pending_authorized_peers: state
                .pending
                .as_ref()
                .map(|pending| pending.next.peer_generations()),
            missing_peers,
            retired_sessions: if state.stopped || state.pending.is_some() {
                connections.len()
            } else {
                0
            },
            stage: progress.as_ref().map(|(stage, _)| *stage),
            blocking_error: progress.and_then(|(_, error)| error),
            accepted_frames: self.accepted_frames.load(Ordering::Relaxed),
            completed_frames: self.completed_frames.load(Ordering::Relaxed),
            rejected_frames: self.rejected_frames.load(Ordering::Relaxed),
            rejected_handshakes: self.rejected_handshakes.load(Ordering::Relaxed),
            historical_key_count: state.seen_public_keys.len(),
        }
    }
}

impl Node {
    /// Exact current public identity policy; None denotes legacy unconfigured
    /// authentication. Fencing/Stopped snapshots confer no current eligibility.
    pub fn peer_authority_snapshot(&self) -> Option<PeerAuthoritySnapshot> {
        let state = self.peer_authority.state.lock().unwrap();
        let phase = if state.stopped {
            PeerFencePhase::Stopped
        } else if state.pending.is_some() {
            PeerFencePhase::Fencing
        } else {
            PeerFencePhase::Ready
        };
        state.era.public_snapshot(&self.node_id, phase)
    }
    /// Atomically retire all current peer sessions. Validates before mutation.
    /// A retry must provide the exact pending authority; admission remains
    /// closed until `advance_peer_authority` completes its old-work/store fence.
    pub fn begin_peer_fence(
        &self,
        next: PeerAuthorityOptions,
    ) -> Result<PeerFenceStatus, PeerFenceError> {
        self.peer_authority.begin(next)?;
        self.peer_changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        Ok(self.peer_fence_status())
    }
    pub fn peer_fence_status(&self) -> PeerFenceStatus {
        self.peer_authority.status()
    }
    /// Hot receiver fence. Request admission is independent and stays in its
    /// existing state. No peer handshake/frame can enter during the transition.
    /// Old accepted work retains authority through real actor/store completion;
    /// timeout/cancellation leaves the exact transition owned and fail-closed.
    pub async fn advance_peer_authority(
        &self,
        next: PeerAuthorityOptions,
        options: PeerFenceOptions,
    ) -> Result<PeerFenceReport, PeerFenceError> {
        let deadline = Instant::now()
            .checked_add(options.timeout)
            .filter(|_| !options.timeout.is_zero())
            .ok_or_else(|| {
                PeerFenceError::InvalidOptions("timeout must be nonzero and representable".into())
            })?;
        self.begin_peer_fence(next)?;
        let future = async {
            let _coordinator = self.peer_authority.coordinator.lock().await;
            let pending = {
                let state = self.peer_authority.state.lock().unwrap();
                if state.stopped {
                    return Err(PeerFenceError::ShuttingDown);
                }
                match &state.pending {
                    Some(pending) => pending.clone(),
                    None => {
                        return state.last_report.clone().ok_or_else(|| {
                            PeerFenceError::InvalidOptions(
                                "no completed authority transition exists".into(),
                            )
                        })
                    }
                }
            };
            *pending.progress.lock().unwrap() = FenceProgress {
                stage: PeerFenceStage::WaitingOldFrames,
                error: None,
            };
            pending.old.gate.wait_for_drain().await;
            // A cancelled prior fence RPC can still own actor/storage work.
            pending.progress.lock().unwrap().stage = PeerFenceStage::WaitingPersistence;
            pending.persistence.wait_for_drain().await;
            pending.progress.lock().unwrap().stage = PeerFenceStage::FlushingStore;
            let actors: Vec<_> = self
                .router
                .actors
                .read()
                .unwrap()
                .values()
                .map(|route| route.actor.clone())
                .collect();
            let mut durability = if actors.is_empty() {
                ApplyDurability::MemoryApplied
            } else {
                ApplyDurability::StableCommitted
            };
            for actor in actors {
                let retention: Arc<dyn Send + Sync> = Arc::new(
                    pending
                        .persistence
                        .try_enter()
                        .map_err(PeerFenceError::Local)?,
                );
                let achieved = flush_fence_retained(
                    &actor,
                    ReplicationAckDurability::StoreCommitted,
                    Some(retention),
                )
                .await
                .map_err(PeerFenceError::Local)?;
                durability =
                    match (durability, achieved) {
                        (ApplyDurability::MemoryApplied, _)
                        | (_, ApplyDurability::MemoryApplied) => ApplyDurability::MemoryApplied,
                        (ApplyDurability::StoreCommitted, _)
                        | (_, ApplyDurability::StoreCommitted) => ApplyDurability::StoreCommitted,
                        _ => ApplyDurability::StableCommitted,
                    };
            }
            pending.persistence.wait_for_drain().await;
            self.storage_executor.drain().await;
            let mut state = self.peer_authority.state.lock().unwrap();
            if state.stopped {
                return Err(PeerFenceError::ShuttingDown);
            }
            let revision = pending.old.revision.checked_add(1).ok_or_else(|| {
                PeerFenceError::InvalidOptions("authority revision exhausted".into())
            })?;
            let report = PeerFenceReport {
                previous_revision: pending.old.revision,
                revision,
                local_generation: pending.next.local_generation(),
                completed_frames: pending.old.gate.completed.load(Ordering::Relaxed),
                local_durability: durability,
                authorized_peers: pending.next.peer_generations(),
                public_authority: PeerAuthoritySnapshot {
                    receiver_id: self.node_id.clone(),
                    revision,
                    phase: PeerFencePhase::Ready,
                    local: pending.next.local.public_identity(),
                    peers: pending.next.peers.clone(),
                },
            };
            for (id, credential) in &pending.next.peers {
                state.high_water.insert(id.clone(), credential.clone());
                state.seen_public_keys.insert(credential.public_key);
            }
            state
                .seen_public_keys
                .insert(pending.next.local.public_identity().public_key);
            state
                .high_water
                .insert(self.node_id.clone(), pending.next.local.public_identity());
            state.era = AuthorityEra::new(revision, Some(pending.next.clone()));
            state.pending = None;
            state.last_report = Some(report.clone());
            Ok(report)
        };
        let result = tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| PeerFenceError::Timeout {
                status: Box::new(self.peer_fence_status()),
            })?;
        if let Err(PeerFenceError::Local(error)) = &result {
            let pending = self.peer_authority.state.lock().unwrap().pending.clone();
            if let Some(pending) = pending {
                *pending.progress.lock().unwrap() = FenceProgress {
                    stage: PeerFenceStage::Blocked,
                    error: Some(error.to_string()),
                };
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ReplicateBatch, ReplicateOp, SessionStart};
    use crate::store::{StoreBinding, StoreError};
    use std::sync::{atomic::AtomicBool, Condvar};

    fn credential(generation: u64, byte: u8) -> PeerCredential {
        PeerCredential::new(generation, [byte; 32])
    }
    fn authority(peer: Option<PeerCredential>) -> PeerAuthorityOptions {
        PeerAuthorityOptions::new(
            credential(1, 1),
            peer.map_or_else(HashMap::new, |peer| {
                HashMap::from([("peer".into(), peer.public_identity())])
            }),
        )
    }
    async fn node(config: NodeConfig) -> Arc<Node> {
        Arc::new(
            Node::start_with_options(
                "local".into(),
                "127.0.0.1:0".parse().unwrap(),
                config,
                NodeRuntimeOptions::default()
                    .with_peer_authority(authority(Some(credential(1, 2)))),
            )
            .await
            .unwrap(),
        )
    }
    fn local_only() -> NodeConfig {
        NodeConfig {
            fragment_map: HashMap::from([(FragmentId(0), vec!["local".into()])]),
            ..Default::default()
        }
    }
    async fn receive(stream: &mut tokio::io::DuplexStream) -> Option<Envelope> {
        let length = stream.read_u32().await.ok()?;
        let mut bytes = vec![0; length as usize];
        stream.read_exact(&mut bytes).await.ok()?;
        Envelope::decode(bytes.as_slice()).ok()
    }
    async fn send(stream: &mut tokio::io::DuplexStream, msg: EnvelopeMsg) -> bool {
        let body = Envelope { msg: Some(msg) }.encode_to_vec();
        stream.write_u32(body.len() as u32).await.is_ok() && stream.write_all(&body).await.is_ok()
    }
    async fn connection(
        node: &Node,
        generation: u64,
        key: u8,
    ) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<bool>) {
        let (mut peer, server) = tokio::io::duplex(64 * 1024);
        let context = node.connection_context();
        let task = tokio::spawn(async move {
            handle_connection(context, Box::pin(server), false, None, None)
                .await
                .was_established
        });
        let handshake = async {
            let Some(Envelope {
                msg: Some(EnvelopeMsg::Hello(local)),
            }) = receive(&mut peer).await
            else {
                return;
            };
            let mut remote = super::super::connection::fresh_hello("peer").unwrap();
            remote.process_generation = generation;
            assert!(send(&mut peer, EnvelopeMsg::Hello(remote.clone())).await);
            let Some(Envelope {
                msg: Some(EnvelopeMsg::HelloAuth(proof)),
            }) = receive(&mut peer).await
            else {
                return;
            };
            assert!(credential(1, 1).public_identity().verify(
                false,
                &remote,
                &local,
                &proof.auth_tag
            ));
            let proof = credential(generation, key).sign(true, &remote, &local);
            assert!(
                send(
                    &mut peer,
                    EnvelopeMsg::HelloAuth(HelloAuth { auth_tag: proof })
                )
                .await
            );
        };
        tokio::time::timeout(Duration::from_secs(2), handshake)
            .await
            .unwrap();
        (peer, task)
    }
    async fn established(node: &Node, task: &tokio::task::JoinHandle<bool>) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !node.peers.lock().await.contains_key("peer") {
                assert!(!task.is_finished(), "peer handshake failed");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    fn batch(key: &str, value: Vec<u8>, timestamp: u64, writer: &str) -> EnvelopeMsg {
        EnvelopeMsg::ReplicateBatch(ReplicateBatch {
            session_id: 7,
            batch_seq: 1,
            fragment: 0,
            required_ack_level: ReplicationAckLevel::Applied as i32,
            ops: vec![ReplicateOp {
                key: Node::qualify_key(key.into()),
                value,
                timestamp_ms: timestamp,
                node_id: writer.into(),
                ..Default::default()
            }],
        })
    }

    #[tokio::test]
    async fn live_old_high_clock_and_old_reconnect_are_fenced_but_historical_rows_are_valid() {
        let node = node(local_only()).await;
        let (mut old, old_task) = connection(&node, 1, 2).await;
        established(&node, &old_task).await;
        node.begin_peer_fence(authority(None)).unwrap();
        let high = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let _ = send(&mut old, batch("poison", vec![9], high, "peer")).await;
        node.advance_peer_authority(authority(None), PeerFenceOptions::default())
            .await
            .unwrap();
        old_task.await.unwrap();
        assert!(node.get("poison".into()).await.unwrap().is_none());
        assert!(node.hlc.lock().await.tick().unwrap().timestamp_ms < high);
        let (old_reconnect, old_reconnect_task) = connection(&node, 1, 2).await;
        assert!(
            !tokio::time::timeout(Duration::from_secs(2), old_reconnect_task)
                .await
                .unwrap()
                .unwrap()
        );
        drop(old_reconnect);
        node.advance_peer_authority(
            authority(Some(credential(2, 3))),
            PeerFenceOptions::default(),
        )
        .await
        .unwrap();
        let (forged, forged_task) = connection(&node, 2, 2).await;
        assert!(!tokio::time::timeout(Duration::from_secs(2), forged_task)
            .await
            .unwrap()
            .unwrap());
        drop(forged);
        let (mut current, current_task) = connection(&node, 2, 3).await;
        established(&node, &current_task).await;
        assert!(
            send(
                &mut current,
                EnvelopeMsg::SessionStart(SessionStart {
                    fragment: 0,
                    session_id: 7
                })
            )
            .await
        );
        assert!(send(&mut current, batch("history", vec![4], 1, "retired-writer")).await);
        assert!(matches!(
            receive(&mut current).await.unwrap().msg,
            Some(EnvelopeMsg::BatchAck(_))
        ));
        let value = node.get("history".into()).await.unwrap().unwrap();
        assert_eq!(value.node_id, "retired-writer");
        assert_eq!(value.value, vec![4]);
        drop(current);
        current_task.await.unwrap();
        node.shutdown().await.unwrap();
    }

    struct GatedActor {
        actual: ActorRef<KvMessage>,
        started: Notify,
        release: Semaphore,
        block_reads: bool,
    }
    struct Gate;
    impl ractor::Actor for Gate {
        type Msg = KvMessage;
        type State = Arc<GatedActor>;
        type Arguments = Arc<GatedActor>;
        async fn pre_start(
            &self,
            _: ActorRef<KvMessage>,
            args: Self::Arguments,
        ) -> Result<Self::State, ractor::ActorProcessingErr> {
            Ok(args)
        }
        async fn handle(
            &self,
            _: ActorRef<KvMessage>,
            message: KvMessage,
            state: &mut Self::State,
        ) -> Result<(), ractor::ActorProcessingErr> {
            let (message, retention) = message.into_retained_parts();
            if matches!(&message, KvMessage::ApplyOp { .. })
                || (state.block_reads
                    && matches!(
                        &message,
                        KvMessage::GetBatchRetained(_) | KvMessage::QueryPage { .. }
                    ))
            {
                state.started.notify_one();
                state.release.acquire().await.unwrap().forget();
            }
            state
                .actual
                .cast(message.with_optional_retention(retention))
                .map_err(|error| ractor::ActorProcessingErr::from(error.to_string()))
        }
    }
    struct ReleaseActor(Arc<GatedActor>);
    impl Drop for ReleaseActor {
        fn drop(&mut self) {
            self.0.release.add_permits(128);
        }
    }

    #[tokio::test]
    async fn decoded_old_apply_and_cancelled_fence_keep_ownership_before_new_era_opens() {
        let node = node(local_only()).await;
        let gate = Arc::new(GatedActor {
            actual: node.router.route_for_fragment(FragmentId(0)).unwrap(),
            started: Notify::new(),
            release: Semaphore::new(0),
            block_reads: false,
        });
        let _release = ReleaseActor(gate.clone());
        let (actor, _) = Gate::spawn(None, Gate, gate.clone()).await.unwrap();
        node.router.insert(FragmentId(0), actor.clone());
        let (mut old, old_task) = connection(&node, 1, 2).await;
        established(&node, &old_task).await;
        send(
            &mut old,
            EnvelopeMsg::SessionStart(SessionStart {
                fragment: 0,
                session_id: 7,
            }),
        )
        .await;
        send(&mut old, batch("accepted", vec![6], 1, "retired-writer")).await;
        tokio::time::timeout(Duration::from_secs(2), gate.started.notified())
            .await
            .unwrap();
        drop(old);
        old_task.abort();
        assert!(old_task.await.unwrap_err().is_cancelled());
        let next = authority(Some(credential(2, 3)));
        let mut waiting =
            Box::pin(node.advance_peer_authority(next.clone(), PeerFenceOptions::default()));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        drop(waiting);
        assert_eq!(node.peer_fence_status().phase(), PeerFencePhase::Fencing);
        assert_eq!(node.peer_fence_status().outstanding_frames(), 1);
        assert!(matches!(
            node.begin_peer_fence(authority(None)),
            Err(PeerFenceError::DifferentPendingAuthority)
        ));
        let (new_early, new_task) = connection(&node, 2, 3).await;
        assert!(!new_task.await.unwrap());
        drop(new_early);
        assert!(matches!(
            node.advance_peer_authority(
                next.clone(),
                PeerFenceOptions::default().with_timeout(Duration::from_millis(20))
            )
            .await,
            Err(PeerFenceError::Timeout { .. })
        ));
        gate.release.add_permits(1);
        let (a, b) = tokio::join!(
            node.advance_peer_authority(next.clone(), PeerFenceOptions::default()),
            node.advance_peer_authority(next.clone(), PeerFenceOptions::default())
        );
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(node.peer_fence_status().outstanding_frames(), 0);
        assert_eq!(
            node.get("accepted".into()).await.unwrap().unwrap().value,
            vec![6]
        );
        assert_eq!(
            node.advance_peer_authority(next, PeerFenceOptions::default())
                .await
                .unwrap()
                .revision(),
            2
        );
        node.shutdown().await.unwrap();
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn authenticated_reconcile_data_retains_old_frame_after_validation_and_imports_history() {
        let node = node(NodeConfig::default()).await;
        let gate = Arc::new(GatedActor {
            actual: node.router.route_for_fragment(FragmentId(0)).unwrap(),
            started: Notify::new(),
            release: Semaphore::new(0),
            block_reads: false,
        });
        let _release = ReleaseActor(gate.clone());
        let (actor, _) = Gate::spawn(None, Gate, gate.clone()).await.unwrap();
        node.router.insert(FragmentId(0), actor.clone());
        let (mut peer, task) = connection(&node, 1, 2).await;
        established(&node, &task).await;
        let mut session = 0;
        let round = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match receive(&mut peer).await.unwrap().msg {
                    Some(EnvelopeMsg::SessionStart(start)) => session = start.session_id,
                    Some(EnvelopeMsg::ReconcileRequest(request)) => break request.round_id,
                    other => panic!("unexpected initial frame {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        let key = Node::qualify_key("fetched-history".into());
        let value = VersionedValue::new(vec![8], 1, "retired-writer".into());
        send(
            &mut peer,
            EnvelopeMsg::ReconcileResponse(ReconcileResponse {
                fragment: 0,
                session_id: session,
                round_id: round,
                page_seq: 0,
                next_after_key: key.clone(),
                snapshot_revision: 1,
                snapshot_time_ms: 10,
                done: true,
                restart: false,
                summaries: vec![KeySummary {
                    key: key.clone(),
                    timestamp_ms: 1,
                    logical_counter: 0,
                    node_id: value.node_id.clone(),
                    is_tombstone: false,
                    content_digest: value.content_digest().to_vec(),
                }],
            }),
        )
        .await;
        let fetch = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(EnvelopeMsg::ReconcileFetch(fetch)) =
                    receive(&mut peer).await.unwrap().msg
                {
                    break fetch;
                }
            }
        })
        .await
        .unwrap();
        send(
            &mut peer,
            EnvelopeMsg::ReconcileData(ReconcileData {
                fragment: 0,
                session_id: session,
                round_id: round,
                chunk_seq: fetch.chunk_seq,
                page_seq: 0,
                done: true,
                entries: vec![SyncEntry {
                    key,
                    value: value.value,
                    timestamp_ms: 1,
                    logical_counter: 0,
                    node_id: value.node_id,
                    expires_at_ms: 0,
                    is_tombstone: false,
                }],
            }),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), gate.started.notified())
            .await
            .unwrap();
        drop(peer);
        task.abort();
        let _ = task.await;
        let next = authority(Some(credential(2, 3)));
        assert!(matches!(
            node.advance_peer_authority(
                next.clone(),
                PeerFenceOptions::default().with_timeout(Duration::from_millis(20))
            )
            .await,
            Err(PeerFenceError::Timeout { .. })
        ));
        assert_eq!(node.peer_fence_status().outstanding_frames(), 1);
        assert_eq!(node.receive_apply_gate.active.load(Ordering::SeqCst), 1);
        gate.release.add_permits(1);
        node.advance_peer_authority(next, PeerFenceOptions::default())
            .await
            .unwrap();
        let imported = node.get("fetched-history".into()).await.unwrap().unwrap();
        assert_eq!(imported.node_id, "retired-writer");
        assert_eq!(imported.value, vec![8]);
        assert_eq!(node.peer_fence_status().outstanding_frames(), 0);
        node.shutdown().await.unwrap();
        actor.stop(None);
        actor.wait(None).await.unwrap();
    }

    #[test]
    fn public_keys_cannot_forge_signatures_and_generation_or_role_changes_invalidate_proof() {
        let signer = credential(1, 2);
        let identity = signer.public_identity();
        let mut peer = super::super::connection::fresh_hello("peer").unwrap();
        peer.process_generation = 1;
        let mut local = super::super::connection::fresh_hello("local").unwrap();
        local.process_generation = 1;
        let proof = signer.sign(true, &peer, &local);
        assert!(identity.verify(true, &peer, &local, &proof));
        assert!(!identity.verify(false, &peer, &local, &proof));
        peer.process_generation = 2;
        assert!(!identity.verify(true, &peer, &local, &proof));
        peer.process_generation = 1;
        local.process_generation = 2;
        assert!(!identity.verify(true, &peer, &local, &proof));
        local.process_generation = 1;
        let forged = PeerCredential::new(1, *identity.public_key()).sign(true, &peer, &local);
        assert!(!identity.verify(true, &peer, &local, &forged));
    }

    #[tokio::test]
    async fn retained_old_session_context_cannot_admit_after_cutoff_or_completed_fence() {
        let node = node(local_only()).await;
        assert!(matches!(
            node.advance_peer_authority(
                authority(None),
                PeerFenceOptions::default().with_timeout(Duration::ZERO)
            )
            .await,
            Err(PeerFenceError::InvalidOptions(_))
        ));
        assert_eq!(node.peer_fence_status().phase(), PeerFencePhase::Ready);
        assert_eq!(
            node.peer_fence_status().missing_peers(),
            &["peer".to_string()]
        );
        let era = node.peer_authority.handshake_era().unwrap();
        let (channel, _data, _responses) = super::super::test_support::test_peer_channel(4);
        let old_session = node.peer_authority.register(&era, "peer", channel).unwrap();
        assert!(node.peer_fence_status().missing_peers().is_empty());
        let work = node.peer_authority.admit(&old_session).unwrap();
        let next = authority(Some(credential(2, 3)));
        let status = node.begin_peer_fence(next.clone()).unwrap();
        assert_eq!(status.outstanding_frames(), 1);
        assert_eq!(status.retired_sessions(), 1);
        assert_eq!(status.pending_revision(), Some(2));
        assert_eq!(
            status.pending_authorized_peers().unwrap()[0].generation(),
            2
        );
        assert!(node.peer_authority.admit(&old_session).is_err());
        assert!(matches!(
            node.advance_peer_authority(
                next.clone(),
                PeerFenceOptions::default().with_timeout(Duration::from_millis(20))
            )
            .await,
            Err(PeerFenceError::Timeout { .. })
        ));
        drop(work);
        let report = node
            .advance_peer_authority(next, PeerFenceOptions::default())
            .await
            .unwrap();
        assert_eq!(report.completed_frames(), 1);
        // The context is deliberately kept alive, independently of stream
        // teardown; its old authority cannot cross into the newly opened era.
        assert!(node.peer_authority.admit(&old_session).is_err());
        assert_eq!(node.peer_fence_status().phase(), PeerFencePhase::Ready);
        assert_eq!(node.peer_fence_status().accepted_frames(), 1);
        assert_eq!(node.peer_fence_status().completed_frames(), 1);
        assert_eq!(node.peer_fence_status().rejected_frames(), 2);
        assert_eq!(
            report.public_authority(),
            &node.peer_authority_snapshot().unwrap()
        );
        assert_eq!(
            report.public_authority().peer_identities()["peer"],
            credential(2, 3).public_identity()
        );
        drop(old_session);
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_old_read_and_sql_pages_keep_authority_until_actor_completion() {
        for sql in [false, true] {
            let node = node(local_only()).await;
            let gate = Arc::new(GatedActor {
                actual: node.router.route_for_fragment(FragmentId(0)).unwrap(),
                started: Notify::new(),
                release: Semaphore::new(0),
                block_reads: true,
            });
            let _release = ReleaseActor(gate.clone());
            let (actor, _) = Gate::spawn(None, Gate, gate.clone()).await.unwrap();
            node.router.insert(FragmentId(0), actor.clone());
            let (mut old, task) = connection(&node, 1, 2).await;
            established(&node, &task).await;
            let request = if sql {
                EnvelopeMsg::QueryPageRequest(crate::proto::QueryPageRequest {
                    req_id: 1,
                    fragment: 0,
                    sql: "SELECT key FROM kv".into(),
                    max_bytes: 1024,
                    ..Default::default()
                })
            } else {
                EnvelopeMsg::ForwardedGet(ForwardedGet {
                    req_id: 1,
                    key: Node::qualify_key("read".into()),
                    ..Default::default()
                })
            };
            send(&mut old, request).await;
            tokio::time::timeout(Duration::from_secs(2), gate.started.notified())
                .await
                .unwrap();
            drop(old);
            task.abort();
            let _ = task.await;
            let next = authority(Some(credential(2, 3)));
            assert!(matches!(
                node.advance_peer_authority(
                    next.clone(),
                    PeerFenceOptions::default().with_timeout(Duration::from_millis(20))
                )
                .await,
                Err(PeerFenceError::Timeout { .. })
            ));
            assert_eq!(node.peer_fence_status().outstanding_frames(), 1);
            gate.release.add_permits(1);
            node.advance_peer_authority(next, PeerFenceOptions::default())
                .await
                .unwrap();
            assert_eq!(node.peer_fence_status().outstanding_frames(), 0);
            node.shutdown().await.unwrap();
            actor.stop(None);
            actor.wait(None).await.unwrap();
        }
    }

    struct BlockingStore {
        entered: Notify,
        release: (StdMutex<bool>, Condvar),
        detached: Arc<AtomicBool>,
        writes: StdMutex<Vec<(Vec<u8>, VersionedValue)>>,
    }
    impl BlockingStore {
        fn release(&self) {
            *self.release.0.lock().unwrap() = true;
            self.release.1.notify_all();
        }
    }
    impl KvStore for BlockingStore {
        fn bind(&self, _: &StoreIdentity) -> Result<StoreBinding, StoreError> {
            let detached = self.detached.clone();
            Ok(StoreBinding::managed(move || {
                detached.store(true, Ordering::SeqCst);
            }))
        }
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }
        fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            self.entered.notify_one();
            let mut release = self.release.0.lock().unwrap();
            while !*release {
                release = self.release.1.wait(release).unwrap();
            }
            self.writes.lock().unwrap().extend_from_slice(writes);
            Ok(())
        }
    }
    struct ReleaseStore(Arc<BlockingStore>);
    impl Drop for ReleaseStore {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    struct FailingStore(AtomicBool);
    impl KvStore for FailingStore {
        fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
            Ok(Vec::new())
        }
        fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
            if self.0.load(Ordering::SeqCst) {
                Err(StoreError("controlled peer fence write failure".into()))
            } else {
                Ok(())
            }
        }
    }
    #[tokio::test]
    async fn failed_peer_fence_keeps_owned_store_buffer_and_reports_blocked_exact_retry() {
        let store = Arc::new(FailingStore(AtomicBool::new(true)));
        let mut config = local_only();
        config.store = Some(store.clone());
        config.flush_batch_size = 10_000;
        config.flush_interval = Duration::from_secs(3600);
        config.snapshot_interval = Duration::from_secs(3600);
        let node = node(config).await;
        let (mut old, task) = connection(&node, 1, 2).await;
        established(&node, &task).await;
        send(
            &mut old,
            EnvelopeMsg::SessionStart(SessionStart {
                fragment: 0,
                session_id: 7,
            }),
        )
        .await;
        send(&mut old, batch("retry", vec![12], 1, "historical-origin")).await;
        assert!(matches!(
            receive(&mut old).await.unwrap().msg,
            Some(EnvelopeMsg::BatchAck(_))
        ));
        let next = authority(Some(credential(2, 3)));
        assert!(matches!(
            node.advance_peer_authority(next.clone(), PeerFenceOptions::default())
                .await,
            Err(PeerFenceError::Local(_))
        ));
        let status = node.peer_fence_status();
        assert_eq!(status.phase(), PeerFencePhase::Fencing);
        assert_eq!(status.stage(), Some(PeerFenceStage::Blocked));
        assert!(status
            .blocking_error()
            .unwrap()
            .contains("controlled peer fence write failure"));
        assert!(node.peer_authority.handshake_era().is_none());
        store.0.store(false, Ordering::SeqCst);
        let report = node
            .advance_peer_authority(next, PeerFenceOptions::default())
            .await
            .unwrap();
        assert_eq!(report.local_durability(), ApplyDurability::StoreCommitted);
        assert_eq!(
            node.get("retry".into()).await.unwrap().unwrap().value,
            vec![12]
        );
        assert!(node.peer_fence_status().blocking_error().is_none());
        task.await.unwrap();
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn forwarded_old_request_in_real_persistence_blocks_fence_after_caller_disappears() {
        let store = Arc::new(BlockingStore {
            entered: Notify::new(),
            release: (StdMutex::new(false), Condvar::new()),
            detached: Arc::new(AtomicBool::new(false)),
            writes: StdMutex::new(Vec::new()),
        });
        let _release = ReleaseStore(store.clone());
        let mut config = local_only();
        config.store = Some(store.clone());
        config.flush_batch_size = 1;
        let node = node(config).await;
        let (mut old, old_task) = connection(&node, 1, 2).await;
        established(&node, &old_task).await;
        send(
            &mut old,
            EnvelopeMsg::ForwardedPut(ForwardedPut {
                req_id: 1,
                key: Node::qualify_key("stored".into()),
                value: vec![5],
                timestamp_ms: 1,
                node_id: "historical-origin".into(),
                ..Default::default()
            }),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        drop(old);
        old_task.abort();
        let _ = old_task.await;
        let next = authority(Some(credential(2, 3)));
        assert!(matches!(
            node.advance_peer_authority(
                next.clone(),
                PeerFenceOptions::default().with_timeout(Duration::from_millis(20))
            )
            .await,
            Err(PeerFenceError::Timeout { .. })
        ));
        assert_eq!(node.peer_fence_status().outstanding_frames(), 1);
        assert!(!store.detached.load(Ordering::SeqCst));
        store.release();
        let report = node
            .advance_peer_authority(next, PeerFenceOptions::default())
            .await
            .unwrap();
        assert_eq!(report.local_durability(), ApplyDurability::StoreCommitted);
        assert!(store
            .writes
            .lock()
            .unwrap()
            .iter()
            .any(|(_, value)| value.value == vec![5]));
        node.shutdown().await.unwrap();
        assert!(store.detached.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_fence_store_rpc_stays_owned_and_shutdown_cannot_reopen_authority() {
        let store = Arc::new(BlockingStore {
            entered: Notify::new(),
            release: (StdMutex::new(false), Condvar::new()),
            detached: Arc::new(AtomicBool::new(false)),
            writes: StdMutex::new(Vec::new()),
        });
        let _release = ReleaseStore(store.clone());
        let mut config = local_only();
        config.store = Some(store.clone());
        config.flush_batch_size = 10_000;
        config.flush_interval = Duration::from_secs(3600);
        config.snapshot_interval = Duration::from_secs(3600);
        let node = node(config).await;
        let (mut old, old_task) = connection(&node, 1, 2).await;
        established(&node, &old_task).await;
        send(
            &mut old,
            EnvelopeMsg::SessionStart(SessionStart {
                fragment: 0,
                session_id: 7,
            }),
        )
        .await;
        send(
            &mut old,
            batch("buffered", vec![11], 1, "historical-origin"),
        )
        .await;
        assert!(matches!(
            receive(&mut old).await.unwrap().msg,
            Some(EnvelopeMsg::BatchAck(_))
        ));
        let next = authority(Some(credential(2, 3)));
        let fencing = node.clone();
        let next_wait = next.clone();
        let waiter = tokio::spawn(async move {
            fencing
                .advance_peer_authority(
                    next_wait,
                    PeerFenceOptions::default().with_timeout(Duration::from_millis(100)),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), store.entered.notified())
            .await
            .unwrap();
        assert!(matches!(
            waiter.await.unwrap(),
            Err(PeerFenceError::Timeout { .. })
        ));
        assert_eq!(node.peer_fence_status().outstanding_persistence(), 1);
        assert!(!store.detached.load(Ordering::SeqCst));
        let mut shutdown = Box::pin(node.shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
                .await
                .is_err()
        );
        assert_eq!(node.peer_fence_status().phase(), PeerFencePhase::Stopped);
        assert_eq!(node.peer_fence_status().revision(), 1);
        assert!(!store.detached.load(Ordering::SeqCst));
        store.release();
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert!(store.detached.load(Ordering::SeqCst));
        assert!(matches!(
            node.advance_peer_authority(next, PeerFenceOptions::default())
                .await,
            Err(PeerFenceError::ShuttingDown)
        ));
        old_task.await.unwrap();
    }

    #[test]
    fn invalid_credentials_and_reused_generations_are_rejected_before_cutoff() {
        let authority = PeerAuthority::new("local".into(), Some(authority(Some(credential(1, 2)))));
        assert!(matches!(
            authority.begin(PeerAuthorityOptions::new(
                credential(1, 1),
                HashMap::from([("peer".into(), credential(2, 2).public_identity())])
            )),
            Err(PeerFenceError::InvalidOptions(_))
        ));
        assert_eq!(authority.status().phase(), PeerFencePhase::Ready);
        assert!(matches!(
            authority.begin(PeerAuthorityOptions::new(
                credential(1, 1),
                HashMap::from([("peer".into(), credential(1, 3).public_identity())])
            )),
            Err(PeerFenceError::InvalidOptions(_))
        ));
        assert!(authority.handshake_era().is_some());
        let invalid = PeerAuthorityOptions::new(credential(0, 1), HashMap::new());
        assert!(invalid.validate().is_err());
        let shared = PeerAuthorityOptions::new(
            credential(1, 1),
            HashMap::from([("peer".into(), credential(1, 1).public_identity())]),
        );
        assert!(shared.validate().is_err());
        assert!(!format!("{:?}", credential(123, 73)).contains("73"));
    }

    #[tokio::test]
    async fn signing_key_from_multiple_rotations_ago_cannot_be_reintroduced() {
        let node = node(local_only()).await;
        node.advance_peer_authority(
            authority(Some(credential(2, 3))),
            PeerFenceOptions::default(),
        )
        .await
        .unwrap();
        node.advance_peer_authority(
            authority(Some(credential(3, 4))),
            PeerFenceOptions::default(),
        )
        .await
        .unwrap();
        let before = node.peer_authority_snapshot().unwrap();
        let old_key_with_new_generation = authority(Some(credential(4, 2)));
        assert!(matches!(
            node.begin_peer_fence(old_key_with_new_generation),
            Err(PeerFenceError::InvalidOptions(_))
        ));
        assert_eq!(node.peer_authority_snapshot().unwrap(), before);
        assert_eq!(node.peer_fence_status().phase(), PeerFencePhase::Ready);
        assert_eq!(node.peer_fence_status().historical_key_count(), 4);
        // Exact unchanged current identities remain eligible, while keys are
        // never recycled after either a generation rotation or revocation.
        let (current, task) = connection(&node, 3, 4).await;
        established(&node, &task).await;
        drop(current);
        task.await.unwrap();
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn historical_signing_keys_cannot_be_reassigned_to_a_different_identity() {
        let node = node(local_only()).await;
        node.advance_peer_authority(
            authority(Some(credential(2, 3))),
            PeerFenceOptions::default(),
        )
        .await
        .unwrap();
        node.advance_peer_authority(authority(None), PeerFenceOptions::default())
            .await
            .unwrap();
        let before = node.peer_authority_snapshot().unwrap();
        for old_key in [2, 3] {
            let renamed = PeerAuthorityOptions::new(
                credential(1, 1),
                HashMap::from([(
                    "new-peer-id".into(),
                    credential(99, old_key).public_identity(),
                )]),
            );
            assert!(matches!(
                node.begin_peer_fence(renamed),
                Err(PeerFenceError::InvalidOptions(_))
            ));
            assert_eq!(node.peer_authority_snapshot().unwrap(), before);
        }
        // A former local signing key cannot become a newly named peer key.
        let swapped_local = PeerAuthorityOptions::new(
            credential(2, 5),
            HashMap::from([("new-peer-id".into(), credential(99, 1).public_identity())]),
        );
        assert!(matches!(
            node.begin_peer_fence(swapped_local),
            Err(PeerFenceError::InvalidOptions(_))
        ));
        assert_eq!(node.peer_authority_snapshot().unwrap(), before);
        node.shutdown().await.unwrap();
    }

    #[test]
    fn historical_key_capacity_exhaustion_rejects_new_keys_without_evicting_tombstones() {
        let registry = PeerAuthority::new("local".into(), Some(authority(Some(credential(1, 2)))));
        let initial_key = credential(1, 2).public_identity().public_key;
        {
            let mut state = registry.state.lock().unwrap();
            let mut value = 0u64;
            while state.seen_public_keys.len() < MAX_HISTORICAL_PEER_KEYS {
                let mut historical = [0; 32];
                historical[..8].copy_from_slice(&value.to_be_bytes());
                state.seen_public_keys.insert(historical);
                value += 1;
            }
        }
        assert!(matches!(
            registry.begin(authority(Some(credential(2, 3)))),
            Err(PeerFenceError::InvalidOptions(_))
        ));
        assert_eq!(registry.status().phase(), PeerFencePhase::Ready);
        assert_eq!(
            registry.status().historical_key_count(),
            MAX_HISTORICAL_PEER_KEYS
        );
        assert!(registry
            .state
            .lock()
            .unwrap()
            .seen_public_keys
            .contains(&initial_key));
        // Revocation introduces no new trust key and must remain possible at
        // capacity. It never makes historical keys available for future reuse.
        registry.begin(authority(None)).unwrap();
        assert_eq!(registry.status().phase(), PeerFencePhase::Fencing);
        assert_eq!(
            registry.status().historical_key_count(),
            MAX_HISTORICAL_PEER_KEYS
        );
    }
}
