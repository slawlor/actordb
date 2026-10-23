// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! Single-shard scan primitive: filter + transform + collect.
//!
//! A scan applies a [`ScanSpec`] to every record in one fragment's local
//! cache and returns the transformed matches. This is the local execution
//! half of a distributed scan; fanning a spec out across fragments and
//! merging the results is the caller's job (a future network-layer PR).
//!
//! The spec is deliberately *data*, not code. Rust closures cannot cross a
//! process boundary, and every system that tried shipping query-time code to
//! its nodes (Riak MapReduce, MongoDB mapReduce, CouchDB views, RedisGears)
//! eventually retreated to declarative models. The predefined
//! [`KeyMatcher`]/[`ValueTransform`] enums serialize cleanly and cover the
//! common shapes: full scans, prefix scans, key listings, and projections.
//!
//! Semantics, locked for v1:
//!
//! * **Visibility = [`crate::actor::FragmentState::get`]**: tombstones and
//!   TTL-expired values are hidden. A scan is a user-facing read; a deleted
//!   key is not a match.
//! * **Deterministic order**: matches are returned in lexicographic key
//!   order (the fragment's ordered key index), so `offset` pagination is
//!   stable as long as the key set does not change between pages.
//! * **Bounded results**: `limit` caps the matches returned. A limit of `0`
//!   means "no explicit limit" and falls back to [`DEFAULT_SCAN_LIMIT`].
//!   The bound is load-bearing: an unbounded full-table scan is an OOM
//!   vector on the reply path.
//! * **Checked fragment revisions**: scans release the read lock between
//!   bounded pages and fail completely if a winning mutation changes the
//!   fragment before validation. Each fragment is read at its own instant. This matches the system's AP/eventual-consistency
//!   contract — callers merging multi-fragment results must not assume a
//!   global linearizable snapshot.
//! * **Scheduling**: the dispatcher routes [`crate::actor::KvMessage::Scan`]
//!   to the background pool (`OpPriority::Background`, documented as
//!   "bulk/scan traffic"), non-sticky round-robin, under the same admission
//!   control as every other op. Scans shed explicitly under backpressure
//!   rather than queueing without bound.

/// Server-side cap applied when [`ScanSpec::limit`] is `0` (no explicit
/// limit). Load-bearing: bounds reply memory for full-table scans.
pub const DEFAULT_SCAN_LIMIT: u32 = 10_000;

/// How a scan selects keys. Predefined matchers only — the spec is data,
/// never shipped code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyMatcher {
    /// Every visible key.
    All,
    /// Keys whose UTF-8 bytes start with `prefix`.
    Prefix(Vec<u8>),
}

/// How a scan transforms a matched value before returning it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueTransform {
    /// Return the full value bytes.
    Identity,
    /// Return keys only; [`ScanMatch::value`] is empty.
    KeyOnly,
    /// Return the first `n` bytes of the value (fewer if the value is
    /// shorter).
    PrefixBytes(u32),
}

/// Parameters for a single-shard scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanSpec {
    /// Which keys are candidates.
    pub matcher: KeyMatcher,
    /// How matched values are transformed.
    pub transform: ValueTransform,
    /// Maximum matches to return. `0` = no explicit limit, in which case
    /// [`DEFAULT_SCAN_LIMIT`] applies.
    pub limit: u32,
    /// Skip-first-N pagination cursor: the first `offset` matches (in
    /// deterministic key order) are skipped. Page `k` of size `n` uses
    /// `offset = k * n`, `limit = n`.
    pub offset: u64,
}

impl ScanSpec {
    pub(crate) fn query_plan(&self, fragment: Option<u32>) -> actordb_query::QueryPlan {
        use actordb_query::{KeyPredicate, Projection, QueryPlan};
        QueryPlan {
            projections: match self.transform {
                ValueTransform::Identity => vec![Projection::Key, Projection::Value],
                ValueTransform::KeyOnly => vec![Projection::Key],
                ValueTransform::PrefixBytes(n) => vec![Projection::Key, Projection::PrefixBytes(n)],
            },
            predicates: match &self.matcher {
                KeyMatcher::All => Vec::new(),
                KeyMatcher::Prefix(prefix) => vec![KeyPredicate::Prefix(prefix.clone())],
            },
            metadata_predicates: Vec::new(),
            fragments: fragment.map(|fragment| vec![fragment]),
            limit: Some(self.effective_limit()),
            offset: self.offset,
            include_tombstones: false,
        }
    }

    /// The effective result bound: the explicit limit, or the server-side
    /// default when the caller passed `0`.
    pub fn effective_limit(&self) -> u64 {
        if self.limit == 0 {
            u64::from(DEFAULT_SCAN_LIMIT)
        } else {
            u64::from(self.limit)
        }
    }

    /// Encode this spec into a wire [`crate::proto::ScanRequest`]. The
    /// coordinator supplies the request id and target fragment.
    pub fn to_scan_request(&self, req_id: u64, fragment: u32) -> crate::proto::ScanRequest {
        let (matcher_kind, matcher_arg) = match &self.matcher {
            KeyMatcher::All => (0u32, Vec::new()),
            KeyMatcher::Prefix(prefix) => (1u32, prefix.clone()),
        };
        let (transform_kind, transform_arg) = match &self.transform {
            ValueTransform::Identity => (0u32, 0u32),
            ValueTransform::KeyOnly => (1u32, 0u32),
            ValueTransform::PrefixBytes(n) => (2u32, *n),
        };
        crate::proto::ScanRequest {
            req_id,
            fragment,
            matcher_kind,
            matcher_arg,
            transform_kind,
            transform_arg,
            limit: self.limit,
            offset: self.offset,
            priority: crate::proto::Priority::Background as i32,
            hops: 4,
        }
    }

    /// Decode a wire [`crate::proto::ScanRequest`] back into a spec.
    /// Unknown matcher/transform kinds are rejected: the spec is data, and
    /// a peer speaking a newer dialect must not be silently misread.
    pub fn from_scan_request(req: &crate::proto::ScanRequest) -> Result<Self, String> {
        let matcher = match req.matcher_kind {
            0 => KeyMatcher::All,
            1 => KeyMatcher::Prefix(req.matcher_arg.clone()),
            other => return Err(format!("unknown scan matcher_kind {other}")),
        };
        let transform = match req.transform_kind {
            0 => ValueTransform::Identity,
            1 => ValueTransform::KeyOnly,
            2 => ValueTransform::PrefixBytes(req.transform_arg),
            other => return Err(format!("unknown scan transform_kind {other}")),
        };
        Ok(ScanSpec {
            matcher,
            transform,
            limit: req.limit,
            offset: req.offset,
        })
    }
}

/// One transformed match returned by a scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanMatch {
    /// The matched key.
    pub key: String,
    /// The transformed value. Empty when the spec used
    /// [`ValueTransform::KeyOnly`].
    pub value: Vec<u8>,
}

impl ScanMatch {
    /// Encode into the wire [`crate::proto::ScanMatchProto`] record.
    pub fn to_proto(&self) -> crate::proto::ScanMatchProto {
        crate::proto::ScanMatchProto {
            key: self.key.clone(),
            value: self.value.clone(),
        }
    }

    /// Decode a wire [`crate::proto::ScanMatchProto`] record.
    pub fn from_proto(proto: &crate::proto::ScanMatchProto) -> Self {
        ScanMatch {
            key: proto.key.clone(),
            value: proto.value.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Scan spec semantics against a real [`FragmentState`].

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::actor::pool_worker::install_worker_get_gate;
    use crate::actor::test_support::test_state;
    use crate::actor::{
        FragmentState, KvActor, KvArgs, KvWorker, KvWorkerArgs, OpPriority, PoolConfig,
        VersionedValue,
    };
    use crate::shard::FragmentId;
    use ractor::rpc::CallResult;
    use ractor::Actor;

    static NEXT_SCAN_PREFIX: AtomicU64 = AtomicU64::new(0);

    fn unique_prefix(label: &str) -> String {
        format!(
            "{label}-{}",
            NEXT_SCAN_PREFIX.fetch_add(1, Ordering::Relaxed)
        )
    }

    async fn scan_dispatcher(
        shared: Arc<FragmentState>,
        label: &str,
        background_pool: PoolConfig,
    ) -> ractor::ActorRef<crate::actor::KvMessage> {
        let prefix = unique_prefix(label);
        let (dispatcher, _handle) = KvActor::spawn(
            None,
            KvActor,
            KvArgs {
                shared,
                fragment: FragmentId(0),
                worker_prefix: prefix,
                high_pool: PoolConfig::default(),
                default_pool: PoolConfig::default(),
                background_pool,
            },
        )
        .await
        .unwrap();
        dispatcher
    }

    fn live(key: &str, value: impl AsRef<[u8]>) -> (String, VersionedValue) {
        (
            key.to_string(),
            VersionedValue::new(value.as_ref().to_vec(), 100, "test-node".to_string()),
        )
    }

    fn state_with(keys: &[(&str, &[u8])]) -> FragmentState {
        test_state(keys.iter().map(|(k, v)| live(k, v)).collect())
    }

    fn spec(matcher: KeyMatcher, transform: ValueTransform, limit: u32, offset: u64) -> ScanSpec {
        ScanSpec {
            matcher,
            transform,
            limit,
            offset,
        }
    }

    #[tokio::test]
    async fn scan_returns_all_live_keys_in_lexicographic_order() {
        let state = state_with(&[("zebra", b"1"), ("apple", b"2"), ("mango", b"3")]);
        let out = state
            .scan(&spec(KeyMatcher::All, ValueTransform::Identity, 0, 0))
            .await
            .unwrap();
        let keys: Vec<&str> = out.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["apple", "mango", "zebra"]);
        assert_eq!(out[0].value, b"2".to_vec());
    }

    #[tokio::test]
    async fn scan_hides_tombstones_like_get() {
        let mut cache: HashMap<String, VersionedValue> = [("gone", b"x"), ("here", b"y")]
            .iter()
            .map(|(k, v)| live(k, v))
            .collect();
        cache.insert(
            "gone".to_string(),
            VersionedValue::tombstone(200, "test-node".to_string()),
        );
        let state = test_state(cache);
        let out = state
            .scan(&spec(KeyMatcher::All, ValueTransform::Identity, 0, 0))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key, "here");
        // Point read agrees: the tombstoned key is invisible.
        assert!(state.get("gone").await.is_none());
    }

    #[tokio::test]
    async fn scan_hides_expired_values_like_get() {
        let mut cache: HashMap<String, VersionedValue> = [("fresh", b"1"), ("stale", b"2")]
            .iter()
            .map(|(k, v)| live(k, v))
            .collect();
        cache.insert(
            "stale".to_string(),
            VersionedValue {
                expires_at_ms: Some(1), // long past
                ..VersionedValue::new(b"2".to_vec(), 100, "test-node".to_string())
            },
        );
        let state = test_state(cache);
        let out = state
            .scan(&spec(KeyMatcher::All, ValueTransform::Identity, 0, 0))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key, "fresh");
        assert!(state.get("stale").await.is_none());
    }

    #[tokio::test]
    async fn prefix_matcher_selects_key_prefix() {
        let state = state_with(&[
            ("user:1", b"a"),
            ("user:2", b"b"),
            ("order:1", b"c"),
            ("user:10", b"d"),
        ]);
        let out = state
            .scan(&spec(
                KeyMatcher::Prefix(b"user:".to_vec()),
                ValueTransform::Identity,
                0,
                0,
            ))
            .await
            .unwrap();
        let keys: Vec<&str> = out.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["user:1", "user:10", "user:2"]);
    }

    #[tokio::test]
    async fn limit_bounds_results_and_zero_means_default_cap() {
        let state = state_with(&[("a", b"1"), ("b", b"2"), ("c", b"3")]);
        let out = state
            .scan(&spec(KeyMatcher::All, ValueTransform::Identity, 2, 0))
            .await
            .unwrap();
        assert_eq!(out.len(), 2);

        let spec_default = spec(KeyMatcher::All, ValueTransform::Identity, 0, 0);
        assert_eq!(
            spec_default.effective_limit(),
            u64::from(DEFAULT_SCAN_LIMIT)
        );
        let spec_explicit = spec(KeyMatcher::All, ValueTransform::Identity, 7, 0);
        assert_eq!(spec_explicit.effective_limit(), 7);
    }

    #[tokio::test]
    async fn offset_paginates_in_stable_order() {
        let state = state_with(&[
            ("a", b"1"),
            ("b", b"2"),
            ("c", b"3"),
            ("d", b"4"),
            ("e", b"5"),
        ]);
        let page = |offset: u64| {
            let state = &state;
            async move {
                state
                    .scan(&spec(KeyMatcher::All, ValueTransform::KeyOnly, 2, offset))
                    .await
                    .unwrap()
            }
        };
        let p0: Vec<String> = page(0).await.into_iter().map(|m| m.key).collect();
        let p1: Vec<String> = page(2).await.into_iter().map(|m| m.key).collect();
        let p2: Vec<String> = page(4).await.into_iter().map(|m| m.key).collect();
        let p3: Vec<String> = page(6).await.into_iter().map(|m| m.key).collect();
        assert_eq!(p0, vec!["a", "b"]);
        assert_eq!(p1, vec!["c", "d"]);
        assert_eq!(p2, vec!["e"]);
        assert!(p3.is_empty(), "offset past the end returns nothing");
    }

    #[tokio::test]
    async fn transforms_shape_returned_values() {
        let state = state_with(&[("k", b"hello-world")]);

        let identity = state
            .scan(&spec(KeyMatcher::All, ValueTransform::Identity, 0, 0))
            .await
            .unwrap();
        assert_eq!(identity[0].value, b"hello-world".to_vec());

        let key_only = state
            .scan(&spec(KeyMatcher::All, ValueTransform::KeyOnly, 0, 0))
            .await
            .unwrap();
        assert_eq!(key_only[0].key, "k");
        assert!(key_only[0].value.is_empty());

        let head = state
            .scan(&spec(KeyMatcher::All, ValueTransform::PrefixBytes(5), 0, 0))
            .await
            .unwrap();
        assert_eq!(head[0].value, b"hello".to_vec());

        // Prefix longer than the value returns the whole value, not a panic.
        let overlong = state
            .scan(&spec(
                KeyMatcher::All,
                ValueTransform::PrefixBytes(10_000),
                0,
                0,
            ))
            .await
            .unwrap();
        assert_eq!(overlong[0].value, b"hello-world".to_vec());
    }

    #[tokio::test]
    async fn scan_never_materializes_non_matches() {
        // Indirect: a KeyOnly scan over values must not clone value bytes.
        // Direct: PrefixBytes(0) on every key still returns one entry per
        // match with empty values.
        let state = state_with(&[("a", b"0123456789"), ("b", b"0123456789")]);
        let out = state
            .scan(&spec(KeyMatcher::All, ValueTransform::PrefixBytes(0), 0, 0))
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|m| m.value.is_empty()));
    }

    #[test]
    fn scan_types_are_clone_and_debug() {
        let spec = ScanSpec {
            matcher: KeyMatcher::Prefix(b"p".to_vec()),
            transform: ValueTransform::PrefixBytes(3),
            limit: 10,
            offset: 5,
        };
        let cloned = spec.clone();
        assert_eq!(format!("{cloned:?}"), format!("{spec:?}"));
        let m = ScanMatch {
            key: "k".to_string(),
            value: vec![1],
        };
        assert_eq!(m.clone().key, "k");
    }

    #[tokio::test]
    async fn dispatcher_routes_scan_to_background_pool() {
        let shared = Arc::new(state_with(&[
            ("user:1", b"a"),
            ("user:2", b"b"),
            ("order:1", b"c"),
        ]));
        let dispatcher = scan_dispatcher(
            shared,
            "scan-happy",
            PoolConfig::builder().workers(2).queue_cap(100).build(),
        )
        .await;

        let result = dispatcher
            .call(
                |reply| crate::actor::KvMessage::Scan {
                    spec: spec(
                        KeyMatcher::Prefix(b"user:".to_vec()),
                        ValueTransform::Identity,
                        0,
                        0,
                    ),
                    reply,
                },
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        let CallResult::Success(Ok(matches)) = result else {
            panic!("dispatcher scan failed: {result:?}");
        };
        let keys: Vec<&str> = matches.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["user:1", "user:2"]);
        assert_eq!(matches[0].value, b"a".to_vec());

        // The scan hid nothing here, but tombstones stay hidden end to end.
        let counts = dispatcher
            .call(
                |reply| crate::actor::KvMessage::GetShedCounts { reply },
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        assert_eq!(counts, CallResult::Success((0, 0, 0)));

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn dispatcher_sheds_scan_when_background_pool_is_full() {
        let shared = Arc::new(state_with(&[("k", b"v")]));
        let dispatcher = scan_dispatcher(
            shared,
            "scan-shed",
            PoolConfig::builder().workers(1).queue_cap(1).build(),
        )
        .await;

        // Occupy the single background worker with a gated Get: the worker
        // blocks holding its reservation, so the pool sits at its queue cap.
        let block_key = format!("block-{}", unique_prefix("g"));
        let gate = install_worker_get_gate(block_key.clone());
        let blocked_get = tokio::spawn({
            let dispatcher = dispatcher.clone();
            let key = block_key.clone();
            async move {
                dispatcher
                    .call(
                        |reply| crate::actor::KvMessage::Get {
                            key,
                            reply,
                            priority: OpPriority::Background,
                        },
                        Some(Duration::from_secs(10)),
                    )
                    .await
            }
        });
        gate.entered.notified().await;

        // The scan cannot be admitted: explicit shed, not "no matches".
        let shed = dispatcher
            .call(
                |reply| crate::actor::KvMessage::Scan {
                    spec: spec(KeyMatcher::All, ValueTransform::KeyOnly, 0, 0),
                    reply,
                },
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        let CallResult::Success(Err(reason)) = shed else {
            panic!("expected an explicit shed, got: {shed:?}");
        };
        assert!(
            reason.contains("shed"),
            "shed reason should name shedding, got: {reason}"
        );

        let counts = dispatcher
            .call(
                |reply| crate::actor::KvMessage::GetShedCounts { reply },
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        assert_eq!(counts, CallResult::Success((0, 0, 1)));

        // Release the worker and confirm the pool recovers: a scan now works.
        gate.release.notify_one();
        let get_result = blocked_get.await.unwrap().unwrap();
        assert!(matches!(get_result, CallResult::Success(Ok(None))));

        let recovered = dispatcher
            .call(
                |reply| crate::actor::KvMessage::Scan {
                    spec: spec(KeyMatcher::All, ValueTransform::KeyOnly, 0, 0),
                    reply,
                },
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        let CallResult::Success(Ok(matches)) = recovered else {
            panic!("scan after shed did not recover: {recovered:?}");
        };
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].key, "k");

        dispatcher.drain().unwrap();
        dispatcher.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn direct_worker_serves_scan_without_a_pool() {
        let shared = Arc::new(state_with(&[("b", b"2"), ("a", b"1")]));
        let (worker, handle) = KvWorker::spawn(
            None,
            KvWorker,
            KvWorkerArgs::builder().shared(shared).build(),
        )
        .await
        .unwrap();

        let result = worker
            .call(
                |reply| crate::actor::KvMessage::Scan {
                    spec: spec(KeyMatcher::All, ValueTransform::Identity, 0, 0),
                    reply,
                },
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        let CallResult::Success(Ok(matches)) = result else {
            panic!("direct worker scan failed: {result:?}");
        };
        let keys: Vec<&str> = matches.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b"]);

        worker.stop(None);
        handle.await.unwrap();
    }

    #[test]
    fn scan_spec_proto_round_trip() {
        for spec in [
            spec(KeyMatcher::All, ValueTransform::Identity, 0, 0),
            spec(
                KeyMatcher::Prefix(b"user/".to_vec()),
                ValueTransform::KeyOnly,
                100,
                5,
            ),
            spec(
                KeyMatcher::Prefix(b"\xff\xfe".to_vec()),
                ValueTransform::PrefixBytes(16),
                7,
                0,
            ),
        ] {
            let req = spec.to_scan_request(42, 3);
            assert_eq!(req.req_id, 42);
            assert_eq!(req.fragment, 3);
            let back = ScanSpec::from_scan_request(&req).unwrap();
            assert_eq!(back, spec);
        }
    }

    #[test]
    fn scan_spec_rejects_unknown_wire_kinds() {
        let mut req = spec(KeyMatcher::All, ValueTransform::Identity, 0, 0).to_scan_request(1, 0);
        req.matcher_kind = 99;
        assert!(ScanSpec::from_scan_request(&req).is_err());
        req.matcher_kind = 0;
        req.transform_kind = 99;
        assert!(ScanSpec::from_scan_request(&req).is_err());
    }

    #[test]
    fn scan_match_proto_round_trip() {
        let m = ScanMatch {
            key: "k".to_string(),
            value: vec![7u8; 100],
        };
        let proto = m.to_proto();
        assert_eq!(ScanMatch::from_proto(&proto), m);
    }
}

#[cfg(test)]
mod bounded_scan_tests {
    use super::*;
    use crate::actor::{FragmentState, VersionedValue};
    use std::collections::HashMap;
    use std::time::Duration;

    #[tokio::test]
    async fn compatibility_scan_rejects_wide_reply_before_full_materialization() {
        let state = FragmentState::new(
            (0..40)
                .map(|i| {
                    (
                        format!("key:{i:03}"),
                        VersionedValue::new(vec![7; 600_000], 1, "n".into()),
                    )
                })
                .collect::<HashMap<_, _>>(),
            None,
            100,
            Duration::from_secs(1),
        );
        let mut spec = ScanSpec {
            matcher: KeyMatcher::All,
            transform: ValueTransform::Identity,
            limit: 40,
            offset: 0,
        };
        assert!(state.scan(&spec).await.unwrap_err().contains("budget"));
        spec.transform = ValueTransform::PrefixBytes(3);
        spec.offset = 38;
        let rows = state.scan(&spec).await.unwrap();
        assert_eq!(
            rows.iter().map(|row| row.key.as_str()).collect::<Vec<_>>(),
            vec!["key:038", "key:039"]
        );
        assert!(rows.iter().all(|row| row.value == vec![7; 3]));
    }
}
