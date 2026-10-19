//! Bounded, independently authenticated request/response streams.

use std::sync::Arc;

use hmac::{KeyInit, Mac};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::engine::Core;
use super::{state, wire, QuorumConfig, QuorumError};
use crate::network::BoxStream;

fn invalid(reason: impl std::fmt::Display) -> QuorumError {
    QuorumError::Invalid(reason.to_string())
}

fn hello(core: &Core) -> Result<wire::Hello, QuorumError> {
    let mut challenge = vec![0; 32];
    getrandom::fill(&mut challenge).map_err(invalid)?;
    Ok(wire::Hello {
        version: 1,
        cluster_id: core.config.cluster_id.clone(),
        node_id: core.id.clone(),
        challenge,
        policy: core.policy.clone(),
    })
}

fn validate_hello(
    core: &Core,
    hello: &wire::Hello,
    expected: Option<&str>,
) -> Result<(), QuorumError> {
    if hello.version != 1
        || hello.cluster_id != core.config.cluster_id
        || hello.policy != core.policy
        || !state::valid_id(&hello.node_id)
        || !core.config.members.contains_key(&hello.node_id)
        || hello.challenge.len() != 32
        || hello.challenge.iter().all(|b| *b == 0)
        || expected.is_some_and(|id| id != hello.node_id)
        || hello.node_id == core.id
    {
        return Err(invalid(
            "quorum handshake identity, version, or policy mismatch",
        ));
    }
    Ok(())
}

fn proof(core: &Core, dialer: &wire::Hello, listener: &wire::Hello, role: &[u8]) -> Vec<u8> {
    let Some(secret) = core.config.cluster_secret.as_deref() else {
        return vec![];
    };
    let mut mac =
        hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).expect("HMAC accepts key lengths");
    mac.update(b"actordb-quorum-proof-v1\0");
    mac.update(role);
    for hello in [dialer, listener] {
        let bytes = hello.encode_to_vec();
        mac.update(&(bytes.len() as u64).to_be_bytes());
        mac.update(&bytes);
    }
    mac.finalize().into_bytes().to_vec()
}

fn verify(
    core: &Core,
    dialer: &wire::Hello,
    listener: &wire::Hello,
    role: &[u8],
    supplied: &[u8],
) -> Result<(), QuorumError> {
    let expected = proof(core, dialer, listener, role);
    if core.config.cluster_secret.is_none() {
        return if supplied.is_empty() {
            Ok(())
        } else {
            Err(invalid("unexpected authentication proof"))
        };
    }
    // MAC a fixed expected proof and verify the supplied proof in constant time.
    let secret = core
        .config
        .cluster_secret
        .as_deref()
        .expect("checked secret");
    let mut expected_mac =
        hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).expect("HMAC accepts key lengths");
    expected_mac.update(&expected);
    let tag = expected_mac.finalize().into_bytes();
    let mut supplied_mac =
        hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).expect("HMAC accepts key lengths");
    supplied_mac.update(supplied);
    supplied_mac.verify_slice(&tag).map_err(invalid)
}

async fn write(
    stream: &mut BoxStream,
    message: wire::frame::Message,
    limit: usize,
) -> Result<(), QuorumError> {
    let frame = wire::Frame {
        message: Some(message),
    };
    if frame.encoded_len() > limit {
        return Err(QuorumError::ResourceLimit("quorum frame bytes".into()));
    }
    let bytes = frame.encode_to_vec();
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(invalid)?;
    stream.write_all(&bytes).await.map_err(invalid)?;
    stream.flush().await.map_err(invalid)
}

async fn read(
    stream: &mut BoxStream,
    core: &Core,
    handshake: bool,
) -> Result<wire::frame::Message, QuorumError> {
    let limit = if handshake { 4096 } else { core.frame_limit() };
    let length = stream.read_u32().await.map_err(invalid)? as usize;
    if length == 0 || length > limit {
        return Err(invalid("quorum frame length"));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await.map_err(invalid)?;
    preflight(&bytes, &core.config, Context::Frame)?;
    wire::Frame::decode(bytes.as_slice())
        .map_err(invalid)?
        .message
        .ok_or_else(|| invalid("empty quorum frame"))
}

pub(super) async fn call(
    core: &Core,
    peer: &str,
    request: wire::Request,
    execute: bool,
) -> Result<wire::Response, QuorumError> {
    let address = core
        .config
        .members
        .get(peer)
        .ok_or(QuorumError::Unavailable)?;
    let timeout = if execute {
        core.config.operation_timeout
    } else {
        core.config.rpc_timeout
    };
    tokio::time::timeout(timeout, async {
        let mut stream = tokio::time::timeout(
            core.config.rpc_timeout,
            core.config.network.connect(*address),
        )
        .await
        .map_err(|_| QuorumError::Unavailable)?
        .map_err(|_| QuorumError::Unavailable)?;
        let dialer = hello(core)?;
        write(
            &mut stream,
            wire::frame::Message::Hello(dialer.clone()),
            4096,
        )
        .await?;
        let wire::frame::Message::Hello(listener) = read(&mut stream, core, true).await? else {
            return Err(invalid("expected quorum hello"));
        };
        validate_hello(core, &listener, Some(peer))?;
        write(
            &mut stream,
            wire::frame::Message::Proof(proof(core, &dialer, &listener, b"dialer")),
            4096,
        )
        .await?;
        let wire::frame::Message::Proof(received) = read(&mut stream, core, true).await? else {
            return Err(invalid("expected quorum proof"));
        };
        verify(core, &dialer, &listener, b"listener", &received)?;
        write(
            &mut stream,
            wire::frame::Message::Request(request),
            core.frame_limit(),
        )
        .await?;
        let wire::frame::Message::Response(response) = read(&mut stream, core, false).await? else {
            return Err(invalid("expected quorum response"));
        };
        Ok(response)
    })
    .await
    .map_err(|_| QuorumError::UnknownOutcome)?
}

pub(super) async fn serve(core: Arc<Core>, mut stream: BoxStream) {
    let result = tokio::time::timeout(
        core.config.operation_timeout + core.config.rpc_timeout,
        async {
            let wire::frame::Message::Hello(dialer) = read(&mut stream, &core, true).await? else {
                return Err(invalid("expected quorum hello"));
            };
            validate_hello(&core, &dialer, None)?;
            let listener = hello(&core)?;
            write(
                &mut stream,
                wire::frame::Message::Hello(listener.clone()),
                4096,
            )
            .await?;
            let wire::frame::Message::Proof(received) = read(&mut stream, &core, true).await?
            else {
                return Err(invalid("expected quorum proof"));
            };
            verify(&core, &dialer, &listener, b"dialer", &received)?;
            write(
                &mut stream,
                wire::frame::Message::Proof(proof(&core, &dialer, &listener, b"listener")),
                4096,
            )
            .await?;
            let wire::frame::Message::Request(request) = read(&mut stream, &core, false).await?
            else {
                return Err(invalid("expected quorum request"));
            };
            if request.node_id != dialer.node_id {
                return Err(invalid("request origin mismatch"));
            }
            // Once decoded, this task owns execution independently of a lost socket.
            let response = core.handle(request).await;
            write(
                &mut stream,
                wire::frame::Message::Response(response),
                core.frame_limit(),
            )
            .await
        },
    )
    .await;
    if let Ok(Err(error)) = result {
        tracing::debug!(%error, "quorum RPC ended");
    }
}

/// Bound repeated allocations before Prost decodes complete snapshots.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Context {
    Frame,
    Request,
    Response,
    Prepare,
    Accept,
    Certificate,
    State,
    Proposal,
    Snapshot,
    Entry,
    Receipt,
    MutationResult,
    Change,
    Command,
    Membership,
    Reconfigure,
    Leaf,
}

pub(super) fn preflight_state(bytes: &[u8], config: &QuorumConfig) -> Result<(), QuorumError> {
    preflight(bytes, config, Context::State)
}

fn preflight(bytes: &[u8], config: &QuorumConfig, root: Context) -> Result<(), QuorumError> {
    fn varint(bytes: &[u8], position: &mut usize) -> Result<u64, QuorumError> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let byte = *bytes
                .get(*position)
                .ok_or_else(|| invalid("truncated quorum varint"))?;
            *position += 1;
            if shift == 63 && byte > 1 {
                return Err(invalid("overflowing quorum varint"));
            }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(invalid("overflowing quorum varint"))
    }
    fn walk(
        bytes: &[u8],
        context: Context,
        config: &QuorumConfig,
        depth: usize,
    ) -> Result<(), QuorumError> {
        use Context::*;
        if depth > 12 {
            return Err(invalid("quorum message nesting"));
        }
        let mut position = 0;
        let mut counts = [0usize; 16];
        let mut variants = 0;
        while position < bytes.len() {
            let tag = varint(bytes, &mut position)?;
            let field = tag >> 3;
            let kind = tag & 7;
            if field == 0 {
                return Err(invalid("invalid quorum protobuf field"));
            }
            if (context == Frame && (1..=4).contains(&field))
                || (context == Request && (4..=7).contains(&field))
                || (context == Command && (2..=7).contains(&field))
            {
                variants += 1;
                if variants > 1 {
                    return Err(invalid("duplicate quorum operation"));
                }
            }
            if let Some(count) = counts.get_mut(field as usize) {
                *count += 1;
                let limit = match (context, field) {
                    (Snapshot, 3) => config.max_records,
                    (Snapshot, 4) => config.max_receipts,
                    (Snapshot, 5) | (Response, 6) => config.max_changes,
                    (Membership, 1 | 2) | (Reconfigure, 1) | (Certificate, 4) => 255,
                    _ => 1,
                };
                if *count > limit {
                    return Err(invalid("quorum repeated field limit"));
                }
            }
            match kind {
                0 => {
                    varint(bytes, &mut position)?;
                }
                1 => {
                    position = position
                        .checked_add(8)
                        .ok_or_else(|| invalid("quorum size overflow"))?;
                }
                5 => {
                    position = position
                        .checked_add(4)
                        .ok_or_else(|| invalid("quorum size overflow"))?;
                }
                2 => {
                    let length = usize::try_from(varint(bytes, &mut position)?).map_err(invalid)?;
                    let end = position
                        .checked_add(length)
                        .ok_or_else(|| invalid("quorum size overflow"))?;
                    let inner = bytes
                        .get(position..end)
                        .ok_or_else(|| invalid("truncated quorum message"))?;
                    let nested = match (context, field) {
                        (Frame, 3) => Request,
                        (Frame, 4) => Response,
                        (Request, 4) => Prepare,
                        (Request, 5) => Accept,
                        (Request, 6) => Certificate,
                        (Request, 7) => Command,
                        (Response, 3) => State,
                        (Response, 4) => MutationResult,
                        (Response, 6) => Change,
                        (Response, 8) => Certificate,
                        (Prepare | Accept, 1) => Certificate,
                        (Accept, 2) => Proposal,
                        (State, 1) => Certificate,
                        (State, 3) => Proposal,
                        (Certificate, 1) | (Proposal, 2) => Snapshot,
                        (Certificate, 3) | (Snapshot, 2) => Membership,
                        (Snapshot, 3) => Entry,
                        (Snapshot, 4) => Receipt,
                        (Snapshot, 5) => Change,
                        (Receipt, 4) => MutationResult,
                        (Command, 6) => Reconfigure,
                        _ => Leaf,
                    };
                    if nested != Leaf {
                        walk(inner, nested, config, depth + 1)?;
                    }
                    position = end;
                }
                _ => return Err(invalid("invalid quorum wire type")),
            }
            if position > bytes.len() {
                return Err(invalid("truncated quorum message"));
            }
        }
        if [Frame, Request, Command].contains(&context) && variants != 1 {
            return Err(invalid("missing quorum operation"));
        }
        Ok(())
    }
    walk(bytes, root, config, 0)
}

#[cfg(test)]
mod tests {
    use super::super::{FileQuorumStore, Membership};
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn preflight_rejects_duplicate_operations_and_excessive_stored_entries() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = QuorumConfig::builder()
            .cluster_id("preflight")
            .store(Arc::new(FileQuorumStore::open(directory.path()).unwrap()))
            .fragment_map(HashMap::from([(
                crate::shard::FragmentId(0),
                vec!["a".into()],
            )]))
            .members(HashMap::from([(
                "a".into(),
                "127.0.0.1:10001".parse().unwrap(),
            )]))
            .build();
        config.max_records = 2;
        let frame = wire::Frame {
            message: Some(wire::frame::Message::Request(wire::Request {
                cluster_id: "preflight".into(),
                node_id: "a".into(),
                fragment: 0,
                operation: Some(wire::request::Operation::Execute(wire::Command {
                    fragment: 0,
                    operation: Some(wire::command::Operation::Get("key".into())),
                })),
            })),
        }
        .encode_to_vec();
        assert!(preflight(&frame, &config, Context::Frame).is_ok());
        let duplicate = [frame.clone(), frame].concat();
        assert!(preflight(&duplicate, &config, Context::Frame).is_err());
        let mut state = state::genesis(
            Membership {
                voters: vec!["a".into()],
                next_voters: vec![],
            },
            crate::shard::FragmentId(0),
            &config.cluster_id,
        );
        state
            .committed
            .as_mut()
            .unwrap()
            .snapshot
            .as_mut()
            .unwrap()
            .entries = vec![wire::Entry::default(); 3];
        assert!(preflight_state(&state.encode_to_vec(), &config).is_err());
    }
}
