//! Deterministic state transitions and protocol validation.

use std::collections::HashSet;

use prost::Message;
use sha2::{Digest, Sha256};

use super::{wire, QuorumConfig, QuorumError};
use crate::shard::{fragment_of, FragmentId};

pub(super) fn ballot_cmp(a: &wire::Ballot, b: &wire::Ballot) -> std::cmp::Ordering {
    (a.round, &a.node_id).cmp(&(b.round, &b.node_id))
}

pub(super) fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 255
}

pub(super) fn validate_membership(m: &wire::Membership) -> Result<(), QuorumError> {
    for voters in [&m.voters, &m.next_voters] {
        if voters.len() > 255
            || voters.iter().any(|id| !valid_id(id))
            || voters.iter().collect::<HashSet<_>>().len() != voters.len()
        {
            return Err(QuorumError::Invalid("invalid or duplicate voters".into()));
        }
    }
    if m.voters.is_empty() {
        return Err(QuorumError::Invalid("empty voter set".into()));
    }
    Ok(())
}

pub(super) fn voters(m: &wire::Membership) -> Vec<String> {
    let mut all = m.voters.clone();
    all.extend(
        m.next_voters
            .iter()
            .filter(|id| !m.voters.contains(id))
            .cloned(),
    );
    all
}

pub(super) fn has_quorum(m: &wire::Membership, acknowledgements: &[String]) -> bool {
    let unique: HashSet<_> = acknowledgements.iter().collect();
    unique.len() == acknowledgements.len()
        && [&m.voters, &m.next_voters].into_iter().all(|group| {
            group.is_empty()
                || group.iter().filter(|id| unique.contains(id)).count() > group.len() / 2
        })
        && unique
            .iter()
            .all(|id| m.voters.contains(id) || m.next_voters.contains(id))
}

pub(super) fn genesis(
    membership: wire::Membership,
    fragment: FragmentId,
    cluster_id: &str,
) -> wire::ReplicaState {
    wire::ReplicaState {
        committed: Some(wire::Certificate {
            snapshot: Some(wire::Snapshot {
                membership: Some(membership),
                fragment: fragment.0,
                cluster_id: cluster_id.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        promise: None,
        accepted: None,
    }
}

pub(super) fn snapshot(c: &wire::Certificate) -> &wire::Snapshot {
    c.snapshot.as_ref().expect("validated certificate snapshot")
}

pub(super) fn committed(state: &wire::ReplicaState) -> &wire::Certificate {
    state
        .committed
        .as_ref()
        .expect("validated committed certificate")
}

pub(super) fn membership(c: &wire::Certificate) -> &wire::Membership {
    snapshot(c)
        .membership
        .as_ref()
        .expect("validated snapshot membership")
}

pub(super) fn validate_snapshot(
    s: &wire::Snapshot,
    fragment: FragmentId,
    config: &QuorumConfig,
) -> Result<(), QuorumError> {
    let invalid = |reason: &str| QuorumError::Invalid(reason.into());
    if s.fragment != fragment.0 || s.cluster_id != config.cluster_id {
        return Err(invalid("snapshot cluster or fragment identity mismatch"));
    }
    validate_membership(
        s.membership
            .as_ref()
            .ok_or_else(|| invalid("missing membership"))?,
    )?;
    if voters(s.membership.as_ref().expect("checked membership"))
        .iter()
        .any(|id| !config.members.contains_key(id))
    {
        return Err(invalid("snapshot voter missing from address book"));
    }
    if s.entries.len() > config.max_records
        || s.receipts.len() > config.max_receipts
        || s.changes.len() > config.max_changes
        || s.change_floor > s.last_change_id
        || (s.slot > 0 && s.command_id.len() != 16)
    {
        return Err(invalid("snapshot exceeds limits or has invalid progress"));
    }
    let fragments = config.fragment_map.len();
    let mut last_key: Option<&str> = None;
    for entry in &s.entries {
        if last_key.is_some_and(|key| key >= entry.key.as_str())
            || fragment_of(&entry.key, fragments) != fragment
        {
            return Err(invalid(
                "snapshot keys are not unique, ordered, or in fragment",
            ));
        }
        let record = entry
            .record
            .as_ref()
            .ok_or_else(|| invalid("missing record"))?;
        if record.revision == 0
            || record.revision > s.slot
            || record.expires_at_ms.is_some_and(|time| time <= s.time_ms)
        {
            return Err(invalid("invalid record revision or expired snapshot value"));
        }
        last_key = Some(&entry.key);
    }
    let mut ids = HashSet::new();
    for receipt in &s.receipts {
        if !valid_id(&receipt.request_id)
            || !ids.insert(&receipt.request_id)
            || receipt.fingerprint.len() != 32
            || receipt.valid_until_ms <= s.time_ms
            || receipt
                .result
                .as_ref()
                .is_none_or(|r| r.revision == 0 || r.revision > s.slot)
        {
            return Err(invalid("invalid mutation receipt"));
        }
    }
    let mut last = s.change_floor;
    for change in &s.changes {
        if change.id
            != last
                .checked_add(1)
                .ok_or_else(|| invalid("change ID overflow"))?
            || change.revision == 0
            || change.revision > s.slot
            || change.time_ms > s.time_ms
            || wire::ChangeKind::try_from(change.kind).is_err()
            || fragment_of(&change.key, fragments) != fragment
        {
            return Err(invalid("invalid change history"));
        }
        last = change.id;
    }
    if last != s.last_change_id {
        return Err(invalid("incomplete change history"));
    }
    Ok(())
}

pub(super) fn validate_certificate(
    c: &wire::Certificate,
    fragment: FragmentId,
    config: &QuorumConfig,
) -> Result<(), QuorumError> {
    let s = c
        .snapshot
        .as_ref()
        .ok_or_else(|| QuorumError::Invalid("missing snapshot".into()))?;
    validate_snapshot(s, fragment, config)?;
    if s.slot == 0 {
        if *c
            != committed(&genesis(
                wire::Membership {
                    voters: config.fragment_map[&fragment].clone(),
                    next_voters: vec![],
                },
                fragment,
                &config.cluster_id,
            ))
            .clone()
        {
            return Err(QuorumError::Invalid("invalid genesis certificate".into()));
        }
        return Ok(());
    }
    let ballot = c
        .ballot
        .as_ref()
        .ok_or_else(|| QuorumError::Invalid("missing ballot".into()))?;
    let voting = c
        .voting_membership
        .as_ref()
        .ok_or_else(|| QuorumError::Invalid("missing voters".into()))?;
    validate_membership(voting)?;
    if voters(voting)
        .iter()
        .any(|id| !config.members.contains_key(id))
    {
        return Err(QuorumError::Invalid(
            "certificate voter missing from address book".into(),
        ));
    }
    if ballot.round == 0
        || !valid_id(&ballot.node_id)
        || !voters(voting).contains(&ballot.node_id)
        || !has_quorum(voting, &c.acknowledgements)
    {
        return Err(QuorumError::Invalid(
            "invalid stable quorum certificate".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_state(
    s: &wire::ReplicaState,
    fragment: FragmentId,
    config: &QuorumConfig,
) -> Result<(), QuorumError> {
    let c = s
        .committed
        .as_ref()
        .ok_or_else(|| QuorumError::Invalid("missing committed state".into()))?;
    validate_certificate(c, fragment, config)?;
    if let Some(promise) = &s.promise {
        if promise.round == 0 || !valid_id(&promise.node_id) {
            return Err(QuorumError::Invalid("invalid promise".into()));
        }
    }
    if let Some(accepted) = &s.accepted {
        let value = accepted
            .snapshot
            .as_ref()
            .ok_or_else(|| QuorumError::Invalid("missing accepted value".into()))?;
        validate_snapshot(value, fragment, config)?;
        let ballot = accepted
            .ballot
            .as_ref()
            .ok_or_else(|| QuorumError::Invalid("missing accepted ballot".into()))?;
        if value.slot
            != snapshot(c)
                .slot
                .checked_add(1)
                .ok_or_else(|| QuorumError::Invalid("slot overflow".into()))?
            || s.promise
                .as_ref()
                .is_none_or(|p| ballot_cmp(p, ballot).is_lt())
        {
            return Err(QuorumError::Invalid(
                "invalid accepted successor or promise".into(),
            ));
        }
    }
    if s.encoded_len() > config.max_state_bytes {
        return Err(QuorumError::ResourceLimit("protocol state bytes".into()));
    }
    Ok(())
}

fn emit(
    s: &mut wire::Snapshot,
    key: String,
    kind: wire::ChangeKind,
    record: Option<wire::Record>,
) -> Result<(), QuorumError> {
    s.last_change_id = s
        .last_change_id
        .checked_add(1)
        .ok_or_else(|| QuorumError::ResourceLimit("change ID exhausted".into()))?;
    s.changes.push(wire::Change {
        id: s.last_change_id,
        key,
        kind: kind as i32,
        record,
        revision: s.slot,
        time_ms: s.time_ms,
    });
    Ok(())
}

/// Physical deletion is safe here because each certified snapshot replaces
/// the entire previous state. Older slots can never merge values back into it.
pub(super) fn maintain(
    s: &mut wire::Snapshot,
    config: &QuorumConfig,
    time_ms: u64,
) -> Result<(), QuorumError> {
    s.time_ms = s.time_ms.max(time_ms);
    let mut expired = Vec::new();
    s.entries.retain(|entry| {
        if entry
            .record
            .as_ref()
            .is_some_and(|r| r.expires_at_ms.is_some_and(|t| t <= s.time_ms))
        {
            expired.push(entry.clone());
            false
        } else {
            true
        }
    });
    for entry in expired {
        emit(s, entry.key, wire::ChangeKind::Expire, entry.record)?;
    }
    s.receipts
        .retain(|receipt| receipt.valid_until_ms > s.time_ms);
    trim_changes(s, config);
    Ok(())
}

fn trim_changes(s: &mut wire::Snapshot, config: &QuorumConfig) {
    let oldest = s
        .time_ms
        .saturating_sub(config.change_retention.as_millis() as u64);
    let mut remove = s
        .changes
        .iter()
        .take_while(|c| c.time_ms < oldest)
        .count()
        .max(s.changes.len().saturating_sub(config.max_changes));
    // Reserve successor/ballot/certificate metadata. Byte pressure shortens
    // event history without dropping valid receipts or live records.
    let budget = config.max_state_bytes / 3 - 64;
    let mut bytes = s.encoded_len();
    for change in &s.changes[..remove] {
        let size = change.encoded_len();
        bytes = bytes.saturating_sub(1 + prost::length_delimiter_len(size) + size);
    }
    while bytes > budget && remove < s.changes.len() {
        let size = s.changes[remove].encoded_len();
        bytes = bytes.saturating_sub(1 + prost::length_delimiter_len(size) + size);
        remove += 1;
    }
    if remove > 0 {
        s.change_floor = s.changes[remove - 1].id;
        s.changes.drain(..remove);
    }
}

pub(super) fn apply_mutation(
    s: &mut wire::Snapshot,
    m: &wire::Mutation,
    config: &QuorumConfig,
) -> Result<wire::MutationResult, QuorumError> {
    if !valid_id(&m.request_id)
        || m.valid_until_ms == 0
        || (m.value.is_none() && m.expires_at_ms.is_some())
        || (m.condition != wire::Condition::Revision as i32 && m.expected_revision != 0)
        || (m.condition == wire::Condition::Revision as i32 && m.expected_revision == 0)
    {
        return Err(QuorumError::Invalid(
            "invalid mutation identity, condition, or expiry".into(),
        ));
    }
    if m.valid_until_ms <= s.time_ms {
        return Err(QuorumError::RequestExpired);
    }
    if m.valid_until_ms.saturating_sub(s.time_ms) > config.max_request_lifetime.as_millis() as u64 {
        return Err(QuorumError::Invalid(
            "request lifetime exceeds configured limit".into(),
        ));
    }
    let fingerprint = Sha256::digest(m.encode_to_vec()).to_vec();
    if let Some(receipt) = s.receipts.iter().find(|r| r.request_id == m.request_id) {
        if receipt.fingerprint != fingerprint {
            return Err(QuorumError::RequestConflict);
        }
        return Ok(receipt.result.as_ref().expect("validated receipt").clone());
    }
    if s.receipts.len() >= config.max_receipts {
        return Err(QuorumError::ResourceLimit(
            "unexpired mutation receipts".into(),
        ));
    }
    if m.expires_at_ms.is_some_and(|time| time <= s.time_ms) {
        return Err(QuorumError::Invalid("write expiry already elapsed".into()));
    }
    let index = s.entries.binary_search_by(|entry| entry.key.cmp(&m.key));
    let existing = index.ok().and_then(|i| s.entries[i].record.clone());
    let matches = match wire::Condition::try_from(m.condition) {
        Ok(wire::Condition::Any) => true,
        Ok(wire::Condition::Absent) => existing.is_none(),
        Ok(wire::Condition::Revision) => existing
            .as_ref()
            .is_some_and(|r| r.revision == m.expected_revision),
        Err(_) => return Err(QuorumError::Invalid("unknown condition".into())),
    };
    let current = if matches {
        let record = m.value.as_ref().map(|value| wire::Record {
            value: value.clone(),
            revision: s.slot,
            expires_at_ms: m.expires_at_ms,
        });
        match (&record, index) {
            (Some(record), Ok(i)) => s.entries[i].record = Some(record.clone()),
            (Some(record), Err(i)) => {
                if s.entries.len() >= config.max_records {
                    return Err(QuorumError::ResourceLimit("live records".into()));
                }
                s.entries.insert(
                    i,
                    wire::Entry {
                        key: m.key.clone(),
                        record: Some(record.clone()),
                    },
                );
            }
            (None, Ok(i)) => {
                s.entries.remove(i);
            }
            (None, Err(_)) => {}
        }
        if record.is_some() || existing.is_some() {
            emit(
                s,
                m.key.clone(),
                if record.is_some() {
                    wire::ChangeKind::Put
                } else {
                    wire::ChangeKind::Delete
                },
                record.clone().or(existing),
            )?;
        }
        record
    } else {
        existing
    };
    let result = wire::MutationResult {
        applied: matches,
        current,
        revision: s.slot,
    };
    s.receipts.push(wire::Receipt {
        request_id: m.request_id.clone(),
        fingerprint,
        valid_until_ms: m.valid_until_ms,
        result: Some(result.clone()),
    });
    trim_changes(s, config);
    Ok(result)
}

pub(super) fn error_response(error: QuorumError) -> wire::Response {
    let status = match &error {
        QuorumError::Invalid(_) => wire::Status::Invalid,
        QuorumError::Storage(_) => wire::Status::Storage,
        QuorumError::Unavailable | QuorumError::ShuttingDown => wire::Status::Unavailable,
        QuorumError::UnknownOutcome => wire::Status::UnknownOutcome,
        QuorumError::RequestExpired => wire::Status::RequestExpired,
        QuorumError::RequestConflict => wire::Status::RequestConflict,
        QuorumError::CursorExpired => wire::Status::CursorExpired,
        QuorumError::ResourceLimit(_) => wire::Status::ResourceLimit,
    };
    wire::Response {
        status: status as i32,
        error: error.to_string(),
        ..Default::default()
    }
}

pub(super) fn check_response(response: &wire::Response) -> Result<(), QuorumError> {
    match wire::Status::try_from(response.status) {
        Ok(wire::Status::Ok) => Ok(()),
        Ok(wire::Status::Storage) => Err(QuorumError::Storage(response.error.clone())),
        Ok(wire::Status::Unavailable) => Err(QuorumError::Unavailable),
        Ok(wire::Status::UnknownOutcome) => Err(QuorumError::UnknownOutcome),
        Ok(wire::Status::RequestExpired) => Err(QuorumError::RequestExpired),
        Ok(wire::Status::RequestConflict) => Err(QuorumError::RequestConflict),
        Ok(wire::Status::CursorExpired) => Err(QuorumError::CursorExpired),
        Ok(wire::Status::ResourceLimit) => Err(QuorumError::ResourceLimit(response.error.clone())),
        _ => Err(QuorumError::Invalid(response.error.clone())),
    }
}
