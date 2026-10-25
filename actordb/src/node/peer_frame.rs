// Copyright (c) Sean Lawlor
// Licensed under the MIT license in the repository root.

//! Session-bound signatures for every post-handshake peer frame.

use super::*;
use ring::signature::{Ed25519KeyPair, UnparsedPublicKey, ED25519};
use sha2::{Digest, Sha256};

pub(super) const SIGNATURE_HEADER_BYTES: usize = crate::wire::PEER_SIGNATURE_HEADER_BYTES;
const DOMAIN: &[u8] = b"actordb-peer-frame-signature-v1\0";

fn signed_message(session: &[u8; 32], dialer: bool, sequence: u64, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(DOMAIN.len() + 73);
    message.extend_from_slice(DOMAIN);
    message.extend_from_slice(session);
    message.push(u8::from(!dialer));
    message.extend_from_slice(&sequence.to_be_bytes());
    message.extend_from_slice(&Sha256::digest(payload));
    message
}
pub(super) struct FrameSigner {
    key: Ed25519KeyPair,
    session: [u8; 32],
    dialer: bool,
}
impl FrameSigner {
    pub(super) fn new(seed: &[u8; 32], dialer: bool, hello: &Hello, listener: &Hello) -> Self {
        Self {
            key: Ed25519KeyPair::from_seed_unchecked(seed).expect("32-byte Ed25519 seed"),
            session: Sha256::digest(super::connection::handshake_transcript(hello, listener))
                .into(),
            dialer,
        }
    }
    pub(super) fn header(&self, sequence: u64, payload: &[u8]) -> [u8; SIGNATURE_HEADER_BYTES] {
        let mut header = [0; SIGNATURE_HEADER_BYTES];
        header[..8].copy_from_slice(&sequence.to_be_bytes());
        header[8..].copy_from_slice(
            self.key
                .sign(&signed_message(
                    &self.session,
                    self.dialer,
                    sequence,
                    payload,
                ))
                .as_ref(),
        );
        header
    }
}
pub(super) struct FrameVerifier {
    public_key: [u8; 32],
    session: [u8; 32],
    dialer: bool,
    next: Option<u64>,
}
impl FrameVerifier {
    pub(super) fn new(public_key: [u8; 32], dialer: bool, hello: &Hello, listener: &Hello) -> Self {
        Self {
            public_key,
            session: Sha256::digest(super::connection::handshake_transcript(hello, listener))
                .into(),
            dialer,
            next: Some(1),
        }
    }
    pub(super) fn verify<'a>(&mut self, body: &'a [u8]) -> Result<&'a [u8], &'static str> {
        if body.len() > MAX_TRANSPORT_FRAME_BYTES {
            return Err("signed peer frame exceeds physical body limit");
        }
        let header = body
            .get(..SIGNATURE_HEADER_BYTES)
            .ok_or("missing signed peer frame header")?;
        let sequence = u64::from_be_bytes(header[..8].try_into().unwrap());
        if self.next != Some(sequence) {
            return Err("peer frame sequence is missing, replayed or reordered");
        }
        let payload = &body[SIGNATURE_HEADER_BYTES..];
        UnparsedPublicKey::new(&ED25519, self.public_key)
            .verify(
                &signed_message(&self.session, self.dialer, sequence, payload),
                &header[8..],
            )
            .map_err(|_| "peer frame session signature failed")?;
        self.next = sequence.checked_add(1);
        Ok(payload)
    }
}
/// Activation waits for actual unsigned HelloAuth delivery. Queue admission
/// alone cannot let a priority application response overtake that flight.
pub(super) struct FrameSigningState {
    signer: StdMutex<Option<FrameSigner>>,
    auth_written: std::sync::atomic::AtomicBool,
    written: Notify,
}
impl FrameSigningState {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            signer: StdMutex::new(None),
            auth_written: std::sync::atomic::AtomicBool::new(false),
            written: Notify::new(),
        })
    }
    pub(super) fn install(&self, signer: FrameSigner) {
        *self.signer.lock().unwrap() = Some(signer);
    }
    pub(super) fn header(
        &self,
        sequence: &mut Option<u64>,
        payload: &[u8],
    ) -> Result<Option<[u8; SIGNATURE_HEADER_BYTES]>, &'static str> {
        let signer = self.signer.lock().unwrap();
        let Some(signer) = signer.as_ref() else {
            return Ok(None);
        };
        let current = sequence.ok_or("peer frame sequence exhausted")?;
        *sequence = current.checked_add(1);
        Ok(Some(signer.header(current, payload)))
    }
    pub(super) fn publish_auth_written(&self) {
        self.auth_written.store(true, Ordering::Release);
        self.written.notify_waiters();
    }
    pub(super) async fn wait_for_auth_written(
        &self,
        deadline: Instant,
        cancel: &mut watch::Receiver<bool>,
    ) -> bool {
        loop {
            let written = self.written.notified();
            if self.auth_written.load(Ordering::Acquire) {
                return true;
            }
            if *cancel.borrow() {
                return false;
            }
            tokio::select! { _ = cancel.changed() => return false,
            result = tokio::time::timeout_at(deadline, written) => if result.is_err() { return false; }, }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::peer_authority::PeerCredential;
    use super::*;

    fn hellos() -> (Hello, Hello) {
        let mut dialer = super::super::connection::fresh_hello("dialer").unwrap();
        dialer.challenge = vec![11; 32];
        dialer.process_generation = 77;
        let mut listener = super::super::connection::fresh_hello("listener").unwrap();
        listener.challenge = vec![22; 32];
        listener.process_generation = 88;
        (dialer, listener)
    }
    fn body(signer: &FrameSigner, sequence: u64, payload: &[u8]) -> Vec<u8> {
        let mut bytes = signer.header(sequence, payload).to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }
    fn verifier(dialer: &Hello, listener: &Hello, role: bool) -> FrameVerifier {
        FrameVerifier::new(
            *PeerCredential::new(77, [3; 32])
                .public_identity()
                .public_key(),
            role,
            dialer,
            listener,
        )
    }

    #[test]
    fn frame_signatures_bind_payload_header_sequence_role_and_fresh_session() {
        let (dialer, listener) = hellos();
        let signer = FrameSigner::new(&[3; 32], true, &dialer, &listener);
        let payload = &[0, 255, b'A', 0, 42];
        let valid = body(&signer, 1, payload);
        let mut check = verifier(&dialer, &listener, true);
        let borrowed = check.verify(&valid).unwrap();
        assert_eq!(borrowed, payload);
        assert_eq!(borrowed.as_ptr(), valid[72..].as_ptr());
        assert!(check.verify(&valid).is_err()); // Same-session replay.
        assert!(check.verify(&body(&signer, 3, payload)).is_err()); // Gap/reorder.
        assert_eq!(check.verify(&body(&signer, 2, payload)).unwrap(), payload);
        for index in [0, 7, 8, 40, 72, valid.len() - 1] {
            let mut tampered = valid.clone();
            tampered[index] ^= 1;
            assert!(verifier(&dialer, &listener, true)
                .verify(&tampered)
                .is_err());
        }
        assert!(verifier(&dialer, &listener, false).verify(&valid).is_err());
        let mut fresh = listener.clone();
        fresh.challenge[0] ^= 1;
        assert!(verifier(&dialer, &fresh, true).verify(&valid).is_err());
        assert!(verifier(&dialer, &listener, true).verify(payload).is_err());
    }

    #[test]
    fn signed_maximum_envelope_retains_original_opaque_budget_and_sequence_never_wraps() {
        let (dialer, listener) = hellos();
        let signer = FrameSigner::new(&[3; 32], true, &dialer, &listener);
        let payload = vec![9; MAX_FRAME_BYTES];
        let bytes = body(&signer, 1, &payload);
        assert_eq!(bytes.len(), MAX_TRANSPORT_FRAME_BYTES);
        assert_eq!(
            verifier(&dialer, &listener, true)
                .verify(&bytes)
                .unwrap()
                .len(),
            MAX_FRAME_BYTES
        );
        let mut oversized = bytes.clone();
        oversized.push(1);
        assert!(verifier(&dialer, &listener, true)
            .verify(&oversized)
            .is_err());
        let mut check = verifier(&dialer, &listener, true);
        check.next = Some(u64::MAX);
        assert!(check.verify(&body(&signer, u64::MAX, &[9])).is_ok());
        assert!(check.verify(&body(&signer, 0, &[9])).is_err());
        let state = FrameSigningState::new();
        state.install(signer);
        let mut sequence = Some(u64::MAX);
        assert!(state.header(&mut sequence, &[1]).is_ok());
        assert!(sequence.is_none());
        assert!(state.header(&mut sequence, &[1]).is_err());
    }

    #[test]
    fn deterministic_frame_signature_vector() {
        let (dialer, listener) = hellos();
        let signer = FrameSigner::new(&[3; 32], true, &dialer, &listener);
        let hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(
            hex(&signer.session),
            "75768063f911128c7dd17a12f81cfae60a10a9b0b9779444062063e452d36b04"
        );
        assert_eq!(
            hex(PeerCredential::new(77, [3; 32])
                .public_identity()
                .public_key()),
            "ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1"
        );
        assert_eq!(hex(&signer.header(1, &[0, 255, b'A', 0, 42])), "00000000000000016a6cc05039e42ed326ee8ab56055fc70aee21bd0d344d0bb6691192db890a33019bd25c4c61d134d223d3c3edd0ece838913d0fee8761181ce44dd94d2480f0b");
    }

    #[tokio::test]
    async fn authentication_write_signal_is_required_before_frame_signing_activation() {
        let state = FrameSigningState::new();
        let (_cancel, mut cancelled) = watch::channel(false);
        assert!(
            !state
                .wait_for_auth_written(Instant::now() + Duration::from_millis(10), &mut cancelled)
                .await
        );
        let mut sequence = Some(1);
        assert!(state.header(&mut sequence, &[1]).unwrap().is_none());
        assert_eq!(sequence, Some(1));
        state.publish_auth_written();
        assert!(
            state
                .wait_for_auth_written(Instant::now() + Duration::from_secs(1), &mut cancelled)
                .await
        );
        let (dialer, listener) = hellos();
        state.install(FrameSigner::new(&[3; 32], true, &dialer, &listener));
        assert!(state.header(&mut sequence, &[1]).unwrap().is_some());
        assert_eq!(sequence, Some(2));
    }
}
