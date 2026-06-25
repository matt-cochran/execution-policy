//! Happy-path overhead + structural-size guards.
//!
//! Run with: `cargo bench --features test-util`

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{ExecutionError, ExecutionPolicyBuilder, Retry};

fn happy_path(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    c.bench_function("run/success/no-policy", |b| {
        let core = TestCore::new(ManualClock::new());
        let policy = ExecutionPolicyBuilder::<u32, &str>::new()
            .retry(Retry::none())
            .build_with(core);
        b.to_async(&rt).iter(|| async {
            let v = policy
                .run(async || Ok::<_, &str>(black_box(7u32)))
                .await
                .unwrap();
            black_box(v);
        });
    });

    c.bench_function("execute/success/full-policy", |b| {
        let core = TestCore::new(ManualClock::new());
        let policy = ExecutionPolicyBuilder::<u32, &str>::new()
            .retry(Retry::exponential().max_attempts(4))
            .attempt_timeout(Duration::from_secs(5))
            .total_timeout(Duration::from_secs(30))
            .build_with(core);
        b.to_async(&rt).iter(|| async {
            let v = policy
                .execute(async |_a| Ok::<_, &str>(black_box(7u32)))
                .await
                .unwrap();
            black_box(v);
        });
    });
}

fn structural_sizes(_c: &mut Criterion) {
    // Guard: ExecutionError stays small (Box<ErrorContext>), so the hot Result
    // does not bloat. These assert at bench time; failures are loud regressions.
    let err = size_of::<ExecutionError<std::io::Error>>();
    let res = size_of::<Result<u32, ExecutionError<std::io::Error>>>();
    assert!(
        err <= 32,
        "ExecutionError<io::Error> grew to {err} bytes (expected <= 32)"
    );
    assert!(
        res <= 32,
        "Result<u32, ExecutionError> grew to {res} bytes (expected <= 32)"
    );
    eprintln!("size_of ExecutionError<io::Error> = {err}, Result = {res}");
}

criterion_group!(benches, structural_sizes, happy_path);
criterion_main!(benches);
