use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde_json::{Value, json};

use tidx::clickhouse::parse_query_response;

/// A `JSONCompact` body shaped like a decoded logs query: quoted 64-bit
/// integers, ISO timestamps and hex strings.
fn logs_response(rows: usize) -> String {
    let meta = json!([
        { "name": "block_num", "type": "UInt64" },
        { "name": "block_timestamp", "type": "DateTime('UTC')" },
        { "name": "log_idx", "type": "UInt32" },
        { "name": "address", "type": "String" },
        { "name": "topic0", "type": "String" },
        { "name": "topic1", "type": "Nullable(String)" },
        { "name": "topic2", "type": "Nullable(String)" },
        { "name": "data", "type": "String" },
    ]);
    let data: Vec<Value> = (0..rows)
        .map(|i| {
            json!([
                (40_000_000 + i).to_string(),
                "2026-10-01T12:00:00Z",
                i % 50,
                format!("0x{:040x}", i % 300),
                format!("0x{:064x}", 0xddf2_52ad_u64),
                format!("0x{:064x}", i % 5000),
                Value::Null,
                format!("0x{:0128x}", i),
            ])
        })
        .collect();
    json!({ "meta": meta, "data": data, "rows": rows }).to_string()
}

fn bench_parse_query_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("clickhouse_decode");

    for rows in [100, 10_000] {
        let body = logs_response(rows);
        group.throughput(Throughput::Bytes(body.len() as u64));
        group.bench_with_input(BenchmarkId::new("logs", rows), &body, |b, body| {
            b.iter(|| parse_query_response(body).unwrap());
        });
    }

    group.finish();
}

criterion_group!(benches, bench_parse_query_response);
criterion_main!(benches);
