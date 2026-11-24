//! Replay captured API traffic against a tidx endpoint and record a latency
//! baseline.
//!
//! Input is JSON lines, one request per line: either `{"path": "/query?…",
//! "t": "<RFC 3339 start time>"}` or a raw Envoy access-log entry (its
//! `x-envoy-origin-path` and `start_time`). Every replayed request carries
//! `x-tidx-caller` (default `replay`) and a `x-request-id` of
//! `<run>-<n>`, so `tidx query-log --by caller` and ClickHouse's
//! query_log show what the replay cost on the server.

use anyhow::{Context, Result, bail};
use clap::{Args as ClapArgs, ValueEnum};
use futures::StreamExt;
use regex_lite::Regex;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

#[derive(ClapArgs)]
pub struct Args {
    /// Captured requests (JSON lines)
    #[arg(long)]
    pub input: PathBuf,

    /// Base URL of the tidx API to replay against, e.g. http://localhost:8080
    #[arg(long)]
    pub url: String,

    /// How to pace requests
    #[arg(long, value_enum, default_value_t = Pace::Recorded)]
    pub pace: Pace,

    /// recorded: replay this many times faster than captured.
    /// rate: requests per second.
    #[arg(long, default_value_t = 1.0)]
    pub speed: f64,

    /// Requests in flight at most (closed loop for --pace max)
    #[arg(long, default_value_t = 64)]
    pub concurrency: usize,

    /// Replay at most this many requests
    #[arg(long)]
    pub limit: Option<usize>,

    /// Rewrite every request's chainId, e.g. to replay mainnet traffic on a
    /// local dev chain
    #[arg(long)]
    pub chain_id: Option<u64>,

    /// Also replay live=true (SSE) requests, reading each for this many
    /// seconds; they are skipped by default
    #[arg(long)]
    pub live_seconds: Option<u64>,

    /// Value of the x-tidx-caller header
    #[arg(long, default_value = "replay")]
    pub caller: String,

    /// Client-side timeout per request, in seconds
    #[arg(long, default_value_t = 60)]
    pub timeout_secs: u64,

    /// Query shapes listed in the report
    #[arg(long, default_value_t = 25)]
    pub top: usize,

    /// Write the full report (JSON) here
    #[arg(long)]
    pub report: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum Pace {
    /// Keep the captured gaps between requests (divided by --speed)
    Recorded,
    /// A fixed number of requests per second (--speed)
    Rate,
    /// As fast as --concurrency allows
    Max,
}

struct Request {
    offset: Duration,
    path: String,
    route: String,
    shape: String,
    live: bool,
}

struct Outcome {
    route: String,
    shape: String,
    status: u16,
    latency_ms: f64,
}

pub async fn run(args: Args) -> Result<()> {
    let (requests, skipped_live) = load(&args)?;
    if requests.is_empty() {
        bail!("no replayable requests in {}", args.input.display());
    }
    let run_id = format!("{}-{:06x}", args.caller, rand::random::<u32>() & 0xff_ffff);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout_secs))
        .pool_max_idle_per_host(args.concurrency)
        .build()?;
    let base = args.url.trim_end_matches('/').to_string();
    eprintln!(
        "replaying {} requests against {base} ({} live skipped), run {run_id}",
        requests.len(),
        skipped_live
    );

    let start = Instant::now();
    let pace = args.pace;
    let speed = args.speed.max(0.0001);
    let live_for = args.live_seconds.map(Duration::from_secs);
    let outcomes: Vec<Outcome> = futures::stream::iter(requests.into_iter().enumerate())
        .map(|(n, req)| {
            let client = client.clone();
            let url = format!("{base}{}", req.path);
            let request_id = format!("{run_id}-{n}");
            let caller = args.caller.clone();
            async move {
                let due = match pace {
                    Pace::Recorded => Some(req.offset.div_f64(speed)),
                    Pace::Rate => Some(Duration::from_secs_f64(n as f64 / speed)),
                    Pace::Max => None,
                };
                if let Some(due) = due {
                    tokio::time::sleep_until((start + due).into()).await;
                }
                let sent = Instant::now();
                let status = send(&client, &url, &caller, &request_id, req.live, live_for).await;
                Outcome {
                    route: req.route,
                    shape: req.shape,
                    status,
                    latency_ms: sent.elapsed().as_secs_f64() * 1000.0,
                }
            }
        })
        .buffer_unordered(args.concurrency.max(1))
        .collect()
        .await;
    let elapsed = start.elapsed();

    let report = build_report(&args, &run_id, &outcomes, skipped_live, elapsed);
    print_summary(&report);
    if let Some(path) = &args.report {
        std::fs::write(path, serde_json::to_string_pretty(&report)?)
            .with_context(|| format!("writing {}", path.display()))?;
        eprintln!("report written to {}", path.display());
    }
    Ok(())
}

/// Status code, 0 for a transport error or client timeout.
async fn send(
    client: &reqwest::Client,
    url: &str,
    caller: &str,
    request_id: &str,
    live: bool,
    live_for: Option<Duration>,
) -> u16 {
    let response = client
        .get(url)
        .header("x-tidx-caller", caller)
        .header("x-request-id", request_id)
        .send()
        .await;
    let Ok(response) = response else { return 0 };
    let status = response.status().as_u16();
    if live {
        // Read the stream for a while, then hang up like a client would.
        let mut response = response;
        let _ = tokio::time::timeout(live_for.unwrap_or_default(), async {
            while let Ok(Some(_)) = response.chunk().await {}
        })
        .await;
    } else if response.bytes().await.is_err() {
        return 0;
    }
    status
}

fn load(args: &Args) -> Result<(Vec<Request>, usize)> {
    let file = std::fs::File::open(&args.input)
        .with_context(|| format!("opening {}", args.input.display()))?;
    let mut parsed = Vec::new();
    let mut skipped_live = 0;
    for line in std::io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let path = entry
            .get("path")
            .or_else(|| entry.get("x-envoy-origin-path"))
            .and_then(Value::as_str);
        let Some(path) = path.filter(|p| p.starts_with('/')) else {
            continue;
        };
        let time = entry
            .get("t")
            .or_else(|| entry.get("start_time"))
            .and_then(Value::as_str)
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok());
        let Some(req) = parse_request(path, args.chain_id) else {
            continue;
        };
        if req.live && args.live_seconds.is_none() {
            skipped_live += 1;
            continue;
        }
        parsed.push((time, req));
        if args.limit.is_some_and(|limit| parsed.len() >= limit) {
            break;
        }
    }
    // Offsets from the earliest captured start; untimed entries go first.
    parsed.sort_by_key(|(time, _)| *time);
    let first = parsed.iter().find_map(|(time, _)| *time);
    let requests = parsed
        .into_iter()
        .map(|(time, mut req)| {
            req.offset = match (time, first) {
                (Some(t), Some(f)) => (t - f).to_std().unwrap_or_default(),
                _ => Duration::ZERO,
            };
            req
        })
        .collect();
    Ok((requests, skipped_live))
}

fn parse_request(path: &str, chain_id: Option<u64>) -> Option<Request> {
    let parsed = url::Url::parse(&format!("http://replay{path}")).ok()?;
    let params: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let get = |key: &str| {
        params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let live = get("live") == Some("true");
    let route = if parsed.path() == "/query" {
        if live {
            "query.live".to_string()
        } else {
            format!(
                "query.{}/{}",
                get("engine").unwrap_or("postgres"),
                get("source").unwrap_or("default")
            )
        }
    } else {
        parsed.path().trim_start_matches('/').to_string()
    };
    let shape = get("sql")
        .map(normalize_sql)
        .unwrap_or_else(|| route.clone());
    let path = match chain_id {
        Some(id) => {
            let mut url = parsed.clone();
            let rewritten: Vec<(String, String)> = params
                .iter()
                .map(|(k, v)| {
                    if k == "chainId" || k == "chain_id" {
                        (k.clone(), id.to_string())
                    } else {
                        (k.clone(), v.clone())
                    }
                })
                .collect();
            url.query_pairs_mut().clear().extend_pairs(rewritten);
            format!("{}?{}", url.path(), url.query().unwrap_or_default())
        }
        None => path.to_string(),
    };
    Some(Request {
        offset: Duration::ZERO,
        path,
        route,
        shape,
        live,
    })
}

/// SQL with literals replaced by `?`, lowercased and whitespace collapsed,
/// so requests that differ only in values group together.
pub fn normalize_sql(sql: &str) -> String {
    static STRINGS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"'(?:[^']|'')*'").unwrap());
    static HEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b0x[0-9a-f]+\b").unwrap());
    static NUMBERS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b\d+(?:\.\d+)?\b").unwrap());
    static LISTS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\?(?:\s*,\s*\?)+").unwrap());
    static SPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());
    let sql = sql.to_lowercase();
    let sql = STRINGS.replace_all(&sql, "?");
    let sql = HEX.replace_all(&sql, "?");
    let sql = NUMBERS.replace_all(&sql, "?");
    let sql = LISTS.replace_all(&sql, "?");
    SPACE.replace_all(sql.trim(), " ").into_owned()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    (sorted[rank.min(sorted.len() - 1)] * 10.0).round() / 10.0
}

fn latency_stats(latencies: &mut [f64]) -> Value {
    latencies.sort_by(f64::total_cmp);
    json!({
        "p50_ms": percentile(latencies, 50.0),
        "p90_ms": percentile(latencies, 90.0),
        "p99_ms": percentile(latencies, 99.0),
        "max_ms": percentile(latencies, 100.0),
    })
}

fn build_report(
    args: &Args,
    run_id: &str,
    outcomes: &[Outcome],
    skipped_live: usize,
    elapsed: Duration,
) -> Value {
    let mut statuses: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_route: BTreeMap<&str, Vec<&Outcome>> = BTreeMap::new();
    let mut by_shape: HashMap<&str, Vec<&Outcome>> = HashMap::new();
    for o in outcomes {
        let key = if o.status == 0 {
            "transport_error".to_string()
        } else {
            o.status.to_string()
        };
        *statuses.entry(key).or_default() += 1;
        by_route.entry(&o.route).or_default().push(o);
        by_shape.entry(&o.shape).or_default().push(o);
    }
    let group = |items: &[&Outcome]| {
        let mut latencies: Vec<f64> = items.iter().map(|o| o.latency_ms).collect();
        let total: f64 = latencies.iter().sum();
        let errors = items.iter().filter(|o| o.status != 200).count();
        let mut stats = latency_stats(&mut latencies);
        stats["requests"] = json!(items.len());
        stats["errors"] = json!(errors);
        stats["total_s"] = json!((total / 100.0).round() / 10.0);
        stats
    };
    let routes: BTreeMap<&str, Value> = by_route
        .iter()
        .map(|(route, items)| (*route, group(items)))
        .collect();
    let mut shapes: Vec<(&str, Vec<&Outcome>)> = by_shape.into_iter().collect();
    let total_ms = |items: &[&Outcome]| items.iter().map(|o| o.latency_ms).sum::<f64>();
    shapes.sort_by(|a, b| total_ms(&b.1).total_cmp(&total_ms(&a.1)));
    let top: Vec<Value> = shapes
        .iter()
        .take(args.top)
        .map(|(shape, items)| {
            let mut stats = group(items);
            stats["route"] = json!(items[0].route);
            stats["shape"] = json!(shape.chars().take(400).collect::<String>());
            stats
        })
        .collect();
    let mut all: Vec<f64> = outcomes.iter().map(|o| o.latency_ms).collect();
    json!({
        "run_id": run_id,
        "url": args.url,
        "input": args.input.display().to_string(),
        "pace": match args.pace { Pace::Recorded => "recorded", Pace::Rate => "rate", Pace::Max => "max" },
        "speed": args.speed,
        "concurrency": args.concurrency,
        "requests": outcomes.len(),
        "skipped_live": skipped_live,
        "elapsed_s": (elapsed.as_secs_f64() * 10.0).round() / 10.0,
        "achieved_rps": ((outcomes.len() as f64 / elapsed.as_secs_f64().max(0.001)) * 10.0).round() / 10.0,
        "statuses": statuses,
        "latency": latency_stats(&mut all),
        "routes": routes,
        "top_shapes_by_total_time": top,
    })
}

fn print_summary(report: &Value) {
    println!(
        "run {}: {} requests in {}s ({} rps), statuses {}",
        report["run_id"].as_str().unwrap_or_default(),
        report["requests"],
        report["elapsed_s"],
        report["achieved_rps"],
        report["statuses"]
    );
    println!("latency {}", report["latency"]);
    if let Some(routes) = report["routes"].as_object() {
        for (route, stats) in routes {
            println!("  {route}: {stats}");
        }
    }
    println!("top shapes by total time:");
    if let Some(top) = report["top_shapes_by_total_time"].as_array() {
        for (i, s) in top.iter().take(10).enumerate() {
            println!(
                "{:>3}. total {}s  n={} p50={} p99={} err={}  {}",
                i + 1,
                s["total_s"],
                s["requests"],
                s["p50_ms"],
                s["p99_ms"],
                s["errors"],
                s["shape"].as_str().unwrap_or_default()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_literals() {
        assert_eq!(
            normalize_sql(
                "SELECT * FROM logs WHERE address = '0xAbC' AND block_num > 123\n AND topic0 IN (1, 2, 3) LIMIT 10"
            ),
            "select * from logs where address = ? and block_num > ? and topic0 in (?) limit ?"
        );
    }

    #[test]
    fn parses_envoy_paths_and_rewrites_chain() {
        let req = parse_request(
            "/query?sql=select+num+from+blocks+limit+1&chainId=4217&engine=clickhouse",
            Some(1337),
        )
        .unwrap();
        assert_eq!(req.route, "query.clickhouse/default");
        assert_eq!(req.shape, "select num from blocks limit ?");
        assert!(req.path.contains("chainId=1337"));
        assert!(!req.live);

        let live = parse_request("/query?sql=select+1&chainId=1&live=true", None).unwrap();
        assert!(live.live);
        assert_eq!(live.route, "query.live");
    }
}
