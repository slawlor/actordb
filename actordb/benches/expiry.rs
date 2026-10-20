//! Fixed lifetime key sets: expiry work with retained tombstones/non-TTL keys.
//! Setup and destruction are outside each sample. Tombstones remain resident;
//! the benchmark measures deadline maintenance, without implying safe age GC.
use actordb::actor::{FragmentState, VersionedValue};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use std::collections::HashMap;
use std::time::Duration;

fn fixture(resident: usize, due: usize, tombstones: bool) -> FragmentState {
    let mut values = HashMap::with_capacity(resident + due);
    for index in 0..resident {
        let value = if tombstones {
            VersionedValue::tombstone(1, "n".into())
        } else {
            VersionedValue::new(vec![1; 16], 1, "n".into())
        };
        values.insert(format!("resident:{index:08}"), value);
    }
    for index in 0..due {
        let mut value = VersionedValue::new(vec![1; 128], 2, "n".into());
        value.expires_at_ms = Some(100);
        values.insert(format!("ttl:{index:08}"), value);
    }
    FragmentState::new(values, None, 1000, Duration::from_secs(1))
}

fn bench_expiry(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("expiry_lifetime");
    for resident in [10_000usize, 100_000] {
        for due in [0usize, 256, 1024] {
            for tombstones in [false, true] {
                let label = if tombstones { "tombstones" } else { "non_ttl" };
                group.bench_with_input(
                    BenchmarkId::new(label, format!("{resident}_resident_{due}_due")),
                    &(resident, due),
                    |b, &(resident, due)| {
                        b.to_async(&rt).iter_batched(
                            || fixture(resident, due, tombstones),
                            |state| async move {
                                let converted = state.expire_values(100).await.unwrap();
                                std::hint::black_box((state, converted))
                            },
                            // Keep only one resident fixture per measured sample.
                            BatchSize::PerIteration,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}
criterion_group!(benches, bench_expiry);
criterion_main!(benches);
