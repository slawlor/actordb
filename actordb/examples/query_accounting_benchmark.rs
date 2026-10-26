//! Fixed resident dataset release diagnostic; setup/shutdown are untimed.
use actordb::actor::VersionedValue;
use actordb::node::{Node, NodeConfig};
use actordb::query::QueryOptions;
use actordb::shard::fragment_of_literal;
use actordb::store::{KvStore, StoreError};
use actordb_query::Projection;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct FixtureStore(Vec<(Vec<u8>, VersionedValue)>);
impl KvStore for FixtureStore {
    fn bind(
        &self,
        _: &actordb::store::StoreIdentity,
    ) -> Result<actordb::store::StoreBinding, StoreError> {
        Ok(actordb::store::StoreBinding::unmanaged())
    }
    fn commit_durability(&self) -> actordb::store::StoreCommitDurability {
        actordb::store::StoreCommitDurability::Buffered
    }
    fn stable_fence(&self) -> Result<actordb::store::StoreCommitDurability, StoreError> {
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
fn physical_key(key: &str, fragments: usize) -> Vec<u8> {
    let encoded: String = key
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut physical = fragment_of_literal(key, fragments).0.to_be_bytes().to_vec();
    physical.extend_from_slice(
        format!("\u{001d}actordb.table.store.v1:4b56:\u{001e}actordb.table.v1:4b56:{encoded}")
            .as_bytes(),
    );
    physical
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let label = std::env::args().nth(1).unwrap_or_else(|| "run".into());
    println!("label,rows,fragments,columns,trial,micros");
    for rows in [8192usize, 16384] {
        for fragments in [1usize, 8, 32, 64] {
            let records = (0..rows)
                .map(|index| {
                    let key = format!("q:{index:06}");
                    (
                        physical_key(&key, fragments),
                        VersionedValue::new(vec![7], 1, "fixture".into()),
                    )
                })
                .collect();
            let node = Node::start(
                format!("accounting-{rows}-{fragments}"),
                "127.0.0.1:0".parse().unwrap(),
                NodeConfig::builder()
                    .fragment_count(fragments)
                    .store(Arc::new(FixtureStore(records)))
                    .build(),
            )
            .await
            .unwrap();
            for columns in [1usize, 8, 32] {
                let mut plan = actordb_query::parse("SELECT key FROM kv").unwrap();
                plan.projections = vec![Projection::Key; columns];
                let options = QueryOptions {
                    max_result_bytes: 128 * 1024 * 1024,
                    max_buffer_bytes: 64 * 1024 * 1024,
                    ..Default::default()
                };
                for trial in 0..4 {
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while node.query_diagnostics().reserved_runtime_bytes != 0 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    let started = Instant::now();
                    let result = node
                        .query_plan_with_options(&plan, options.clone())
                        .await
                        .unwrap();
                    let micros = started.elapsed().as_micros();
                    assert_eq!(result.rows.len(), rows);
                    assert!(result.rows.iter().all(|row| row.len() == columns));
                    if trial != 0 {
                        println!("{label},{rows},{fragments},{columns},{trial},{micros}");
                    }
                }
            }
            node.shutdown().await.unwrap();
        }
    }
}
