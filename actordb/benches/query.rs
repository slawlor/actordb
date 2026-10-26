//! Selective queries over fixed resident key sets and selected fragment counts.
use actordb::actor::VersionedValue;
use actordb::node::{Node, NodeConfig};
use actordb::shard::fragment_of_literal;
use actordb::store::{KvStore, StoreError};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

struct FixtureStore(Vec<(Vec<u8>, VersionedValue)>);
impl KvStore for FixtureStore {
    fn bind(
        &self,
        _identity: &actordb::store::StoreIdentity,
    ) -> Result<actordb::store::StoreBinding, actordb::store::StoreError> {
        Ok(actordb::store::StoreBinding::unmanaged())
    }

    fn commit_durability(&self) -> actordb::store::StoreCommitDurability {
        actordb::store::StoreCommitDurability::Buffered
    }

    fn stable_fence(
        &self,
    ) -> Result<actordb::store::StoreCommitDurability, actordb::store::StoreError> {
        Ok(self.commit_durability())
    }
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self.0.clone())
    }
    fn load_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError> {
        Ok(self
            .0
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .cloned()
            .collect())
    }
    fn write_batch(&self, _: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError> {
        Ok(())
    }
}
fn bench_query(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("query_selectivity");
    for fragments in [1usize, 16, 128] {
        let values = (0..10_000)
            .map(|index| {
                let key = format!("key:{index:06}");
                let encoded: String = key
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                let mut stored = fragment_of_literal(&key, fragments).0.to_be_bytes().to_vec();
                stored.extend_from_slice(
                    format!("\u{001d}actordb.table.store.v1:4b56:\u{001e}actordb.table.v1:4b56:{encoded}")
                        .as_bytes(),
                );
                (
                    stored,
                    VersionedValue::new(vec![1; 100], actordb::actor::now_ms(), "fixture".into()),
                )
            })
            .collect();
        let node = rt
            .block_on(Node::start(
                format!("query-bench-{fragments}"),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::builder()
                    .fragment_count(fragments)
                    .store(Arc::new(FixtureStore(values)))
                    .build(),
            ))
            .unwrap();
        for (label, sql) in [
            ("exact_late", "SELECT key FROM kv WHERE key = 'key:009999'"),
            (
                "range_late",
                "SELECT key FROM kv WHERE key >= 'key:009980' AND key < 'key:009990'",
            ),
            (
                "prefix_late",
                "SELECT key FROM kv WHERE STARTS_WITH(key, 'key:00999')",
            ),
            ("one_fragment", "SELECT COUNT(*) FROM kv WHERE frag = 0"),
            ("all_fragments", "SELECT COUNT(*) FROM kv"),
        ] {
            let plan = actordb_query::parse(sql).unwrap();
            // Validate resident fixture routing outside the timed loop. An
            // incorrectly encoded store fixture must not benchmark empty scans.
            let result = rt.block_on(node.query_plan(&plan)).unwrap();
            match label {
                "exact_late" => assert_eq!(result.rows.len(), 1),
                "range_late" | "prefix_late" => assert_eq!(result.rows.len(), 10),
                "all_fragments" => assert_eq!(
                    result.rows,
                    vec![vec![actordb_query::QueryValue::Integer(10_000)]]
                ),
                "one_fragment" => {
                    let count = (0..10_000)
                        .filter(|index| {
                            fragment_of_literal(&format!("key:{index:06}"), fragments).0 == 0
                        })
                        .count();
                    assert_eq!(
                        result.rows,
                        vec![vec![actordb_query::QueryValue::Integer(count as i128)]]
                    );
                }
                _ => unreachable!(),
            }
            group.bench_with_input(BenchmarkId::new(label, fragments), &plan, |b, plan| {
                b.to_async(&rt)
                    .iter(|| async { std::hint::black_box(node.query_plan(plan).await.unwrap()) });
            });
        }
        rt.block_on(node.shutdown()).unwrap();
    }
    group.finish();
}
criterion_group!(benches, bench_query);
criterion_main!(benches);
