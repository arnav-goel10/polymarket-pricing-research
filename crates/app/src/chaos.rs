//! The HA chaos check: the fault plan, the files a run leaves behind, and
//! the verdict computed from them. Process control lives in `bin/chaos`.
//!
//! A run directory holds:
//!
//! | File | Written by | What |
//! | --- | --- | --- |
//! | `plan.json` | `plan`, `local` | the faults to run |
//! | `faults.jsonl` | the harness | each fault as executed, with when its node was ready again |
//! | `harness.json` | the harness | `invalid` when the run cannot count (A7) |
//! | `client.json` | `client`, `local` | client counters, merger totals, connection changes |
//! | `log.jsonl` | `client`, `local` | the delivered log: every Deliver and Late verdict. Private, never an artifact |
//! | `report.json` | `verify` | counts and timings only |
//!
//! Truth is a `.prec` file: the simulator's manifest in CI.
//!
//! The window W is cut by the venue timestamp of each key. T0 is `margin`
//! after every dialed subscription is live, T1 is `margin` after the last
//! recovery, both on the wall clock. Per feed, a wall time t maps to the
//! newest venue timestamp among that feed's truth keys broadcast by t, and
//! a key is in W when `map(T0) < ts <= map(T1)`. For synthetic frames the
//! venue timestamp is the broadcast time, so this is `T0 <= ts < T1` to the
//! millisecond. For a played recording, whose timestamps are from the day
//! it was recorded, the map keeps every key broadcast before T0 out of W.
//!
//! Checks, and the order in which the first failure names the reason:
//!
//! | Reason | Rule |
//! | --- | --- |
//! | `seq_gap`, `resume`, `unkeyed` | client counters are 0 (A6) |
//! | `missing` | every timed truth key in W was delivered (A1) |
//! | `duplicate` | no timed key was delivered twice in the whole run (A2) |
//! | `extra` | every delivered key is in the truth (A3) |
//! | `faults_not_observed` | executed equals planned, and the client saw each victim go down (A4) |
//! | `vacuous` | each node delivered first copies and truth events exist inside every fault (A5) |
//!
//! A6 comes first because a broken stream also loses events, and the
//! protocol counter is the precise name. `vacuous` comes last because it
//! only matters for a run that would otherwise pass. A7 (two nodes down at
//! once without the harness meaning it) makes the run invalid, exit 2.

use ha::client::{ClientCounters, Event};
use ha::key::{frame_keys, EventKey, Feed, KeyKind};
use ha::merge::{FeedCounters, Verdict};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Write};
use std::path::Path;

pub const PLAN: &str = "plan.json";
pub const FAULTS: &str = "faults.jsonl";
pub const HARNESS: &str = "harness.json";
pub const CLIENT: &str = "client.json";
pub const LOG: &str = "log.jsonl";
pub const REPORT: &str = "report.json";

/// Delay after the fault instant over which added delay is measured.
const DELAY_WINDOW_MS: i64 = 5_000;
/// How long after a fault's recovery the client may report the victim down.
const OBSERVE_SLACK_MS: i64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    /// SIGKILL, respawn after 1 s.
    Kill9,
    /// SIGSTOP, then SIGCONT after `freeze_ms`.
    Freeze,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedFault {
    pub node: usize,
    pub kind: FaultKind,
    /// Milliseconds after T0.
    pub at_ms: i64,
    pub freeze_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub seed: u64,
    pub margin_ms: i64,
    pub faults: Vec<PlannedFault>,
    /// The seeded fault this run carries, if any.
    pub mutant: Option<String>,
    /// Faults start without waiting for the peer (the overlap-kill mutant).
    pub overlap: bool,
}

/// The harness mutant: every fault a kill, 300 ms apart, no readiness gate.
pub const OVERLAP_KILL: &str = "overlap-kill";

fn mix(seed: u64, i: u64) -> u64 {
    let mut z = seed ^ i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Faults alternate nodes, and kinds go kill, freeze, freeze, kill, so each
/// node gets both kinds. Fault i is due `1 s + i * gap` after T0 plus a
/// jitter below a quarter gap from the seed.
pub fn plan(
    seed: u64,
    faults: usize,
    gap_ms: i64,
    freeze_ms: u64,
    margin_ms: i64,
    mutant: Option<&str>,
) -> Plan {
    let overlap = mutant == Some(OVERLAP_KILL);
    let faults = (0..faults)
        .map(|i| {
            let kind = match (overlap, i % 4) {
                (true, _) | (false, 0 | 3) => FaultKind::Kill9,
                _ => FaultKind::Freeze,
            };
            let at_ms = if overlap {
                1_000 + 300 * i as i64
            } else {
                let jitter = (mix(seed, i as u64) % (gap_ms.max(4) as u64 / 4)) as i64;
                1_000 + gap_ms * i as i64 + jitter
            };
            PlannedFault {
                node: i % 2,
                kind,
                at_ms,
                freeze_ms,
            }
        })
        .collect();
    Plan {
        seed,
        margin_ms,
        faults,
        mutant: mutant.map(str::to_string),
        overlap,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Executed {
    pub node: usize,
    pub kind: FaultKind,
    pub at_wall_ms: i64,
    pub at_mono_ms: i64,
    /// When the node's `/readyz` answered 200 again.
    pub recovered_wall_ms: i64,
    pub recovered_mono_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Harness {
    pub invalid: Option<String>,
}

/// A client connection going live or down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnObs {
    pub node: usize,
    pub live: bool,
    pub wall_ms: i64,
    pub mono_ms: i64,
    /// For a down: when the last `ev` or `hb` came from that node.
    pub last_msg_mono_ms: Option<i64>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counters {
    pub seq_gaps: u64,
    pub resume_violations: u64,
    pub unkeyed: u64,
    pub resets: u64,
    pub reconnects: u64,
    pub incomplete_snapshots: u64,
    pub order_inversions: u64,
    pub self_repeats: u64,
    pub disconnects: u64,
    pub frames: u64,
}

impl From<ClientCounters> for Counters {
    fn from(c: ClientCounters) -> Self {
        Counters {
            seq_gaps: c.seq_gaps,
            resume_violations: c.resume_violations,
            unkeyed: c.unkeyed,
            resets: c.resets,
            reconnects: c.reconnects,
            incomplete_snapshots: c.incomplete_snapshots,
            order_inversions: c.order_inversions,
            self_repeats: c.self_repeats,
            disconnects: c.disconnects,
            frames: c.frames,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergerTotals {
    pub delivered: u64,
    pub duplicates: u64,
    pub late: u64,
    pub expired: u64,
    pub forced_evictions: u64,
    pub untimed: u64,
}

impl From<FeedCounters> for MergerTotals {
    fn from(c: FeedCounters) -> Self {
        MergerTotals {
            delivered: c.delivered,
            duplicates: c.duplicates,
            late: c.late,
            expired: c.expired,
            forced_evictions: c.forced_evictions,
            untimed: c.untimed,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRecord {
    /// Nodes the client was meant to dial.
    pub wanted: Vec<bool>,
    /// When every wanted subscription was first live at once.
    pub live_wall_ms: Option<i64>,
    pub live_mono_ms: Option<i64>,
    pub conn: Vec<ConnObs>,
    pub counters: Counters,
    pub first_from: Vec<u64>,
    pub merger: MergerTotals,
}

/// One line of the delivered log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    /// The key as text, see [`key_text`].
    pub k: String,
    /// Feed index.
    pub f: u8,
    pub ts: Option<i64>,
    /// true for Deliver, false for Late.
    pub d: bool,
    pub n: usize,
    /// Wall time of the verdict, microseconds.
    pub w: i64,
    /// The node's receive time, microseconds.
    pub rx: i64,
    /// Monotonic milliseconds since the run started.
    pub m: i64,
}

/// A key as text, the same for the log and the truth.
pub fn key_text(k: &EventKey) -> String {
    match k.ts_millis {
        Some(t) => format!("{:?}/{:032x}/{t}", k.kind, k.id),
        None => format!("{:?}/{:032x}/-", k.kind, k.id),
    }
}

/// The log lines for one event: Deliver and Late verdicts only.
pub fn log_lines(e: &Event, wall_us: i64, mono_ms: i64, out: &mut Vec<LogLine>) {
    for (k, v) in &e.keys {
        if *v == Verdict::Duplicate {
            continue;
        }
        out.push(LogLine {
            k: key_text(k),
            f: k.kind.feed().index() as u8,
            ts: k.ts_millis,
            d: *v == Verdict::Deliver,
            n: e.node,
            w: wall_us,
            rx: e.rx_micros,
            m: mono_ms,
        });
    }
}

/// One truth frame: broadcast time and its keys.
#[derive(Debug, Clone)]
pub struct TruthFrame {
    pub rx_us: i64,
    pub keys: Vec<(String, u8, Option<i64>)>,
}

pub fn load_truth(path: &Path) -> io::Result<Vec<TruthFrame>> {
    let (recs, _) = feeds::recording::read_all(path)
        .map_err(|e| io::Error::other(format!("{path:?}: {e:?}")))?;
    let mut keys = Vec::new();
    Ok(recs
        .into_iter()
        .filter_map(|r| {
            keys.clear();
            frame_keys(r.source, &r.frame, &mut keys);
            if keys.is_empty() {
                return None;
            }
            Some(TruthFrame {
                rx_us: r.recv_micros,
                keys: keys
                    .iter()
                    .map(|k| (key_text(k), k.kind.feed().index() as u8, k.ts_millis))
                    .collect(),
            })
        })
        .collect())
}

pub fn write_json<T: Serialize>(path: &Path, v: &T) -> io::Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(v)?)
}

pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<T> {
    let b = std::fs::read(path).map_err(|e| io::Error::other(format!("{path:?}: {e}")))?;
    serde_json::from_slice(&b).map_err(|e| io::Error::other(format!("{path:?}: {e}")))
}

pub fn write_jsonl<T: Serialize>(path: &Path, items: &[T]) -> io::Result<()> {
    let mut w = io::BufWriter::new(std::fs::File::create(path)?);
    for i in items {
        serde_json::to_writer(&mut w, i)?;
        w.write_all(b"\n")?;
    }
    w.flush()
}

pub fn read_jsonl<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<Vec<T>> {
    let f = std::fs::File::open(path).map_err(|e| io::Error::other(format!("{path:?}: {e}")))?;
    let mut out = Vec::new();
    for line in io::BufReader::new(f).lines() {
        let line = line?;
        if !line.trim().is_empty() {
            out.push(serde_json::from_str(&line).map_err(io::Error::other)?);
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Spread {
    pub n: usize,
    pub p50: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
}

fn spread(mut v: Vec<f64>) -> Spread {
    v.sort_by(|a, b| a.total_cmp(b));
    let at = |q: f64| -> Option<f64> {
        if v.is_empty() {
            return None;
        }
        let i = ((v.len() - 1) as f64 * q).round() as usize;
        Some(v[i])
    };
    Spread {
        n: v.len(),
        p50: at(0.5),
        p99: at(0.99),
        max: v.last().copied(),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Late {
    pub already_delivered: u64,
    pub pre_window: u64,
    pub lost: u64,
    pub post_window: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FaultCounts {
    pub planned: usize,
    pub executed: usize,
    pub kill9: usize,
    pub freeze: usize,
    pub observed: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Outcome {
    /// `pass`, `fail` or `invalid`.
    pub result: String,
    pub reason: Option<String>,
    /// Every check that failed, in reason order.
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub verdict: Outcome,
    pub mutant: Option<String>,
    pub faults: FaultCounts,
    pub window_ms: i64,
    pub truth_keys_in_window: usize,
    pub missing: u64,
    pub duplicates: u64,
    pub extra: u64,
    pub late: Late,
    pub added_delay_ms: Spread,
    pub rehedge_ms: Spread,
    pub binance_agg_id_gaps: u64,
    pub first_from: Vec<u64>,
    pub fault_windows_without_truth: usize,
    pub counters: Counters,
    pub merger: MergerTotals,
}

impl Report {
    pub fn exit_code(&self) -> i32 {
        match self.verdict.result.as_str() {
            "pass" => 0,
            "invalid" => 2,
            _ => 1,
        }
    }
}

/// Everything a verdict is computed from.
pub struct Inputs {
    pub plan: Plan,
    pub executed: Vec<Executed>,
    pub harness: Harness,
    pub client: ClientRecord,
    pub log: Vec<LogLine>,
    pub truth: Vec<TruthFrame>,
}

pub fn load(run: &Path, truth: &Path) -> io::Result<Inputs> {
    Ok(Inputs {
        plan: read_json(&run.join(PLAN))?,
        executed: if run.join(FAULTS).exists() {
            read_jsonl(&run.join(FAULTS))?
        } else {
            Vec::new()
        },
        harness: if run.join(HARNESS).exists() {
            read_json(&run.join(HARNESS))?
        } else {
            Harness::default()
        },
        client: read_json(&run.join(CLIENT))?,
        log: read_jsonl(&run.join(LOG))?,
        truth: load_truth(truth)?,
    })
}

/// Per feed, the newest venue timestamp among truth keys broadcast at or
/// before `wall_ms`.
fn ts_at(truth: &[TruthFrame], wall_ms: i64) -> [Option<i64>; 3] {
    let mut out = [None::<i64>; 3];
    for f in truth {
        if f.rx_us > wall_ms.saturating_mul(1_000) {
            continue;
        }
        for (_, feed, ts) in &f.keys {
            if let Some(t) = ts {
                let o = &mut out[*feed as usize];
                *o = Some(o.map_or(*t, |x: i64| x.max(*t)));
            }
        }
    }
    out
}

fn in_window(lo: &[Option<i64>; 3], hi: &[Option<i64>; 3], feed: u8, ts: i64) -> bool {
    let f = feed as usize;
    lo[f].is_none_or(|l| ts > l) && hi[f].is_some_and(|h| ts <= h)
}

fn binance_trade_gaps<'a>(keys: impl Iterator<Item = &'a str>) -> u64 {
    let prefix = format!("{:?}/", KeyKind::BinanceTrade);
    let mut by_symbol: HashMap<u64, Vec<u64>> = HashMap::new();
    for k in keys {
        let Some(rest) = k.strip_prefix(&prefix) else {
            continue;
        };
        let Some(id) = rest
            .split('/')
            .next()
            .and_then(|h| u128::from_str_radix(h, 16).ok())
        else {
            continue;
        };
        by_symbol
            .entry((id >> 64) as u64)
            .or_default()
            .push(id as u64);
    }
    let mut gaps = 0;
    for ids in by_symbol.values_mut() {
        ids.sort_unstable();
        ids.dedup();
        gaps += ids.windows(2).filter(|w| w[1] != w[0] + 1).count() as u64;
    }
    gaps
}

pub fn verify(inp: &Inputs) -> Report {
    let mut r = Report {
        mutant: inp.plan.mutant.clone(),
        counters: inp.client.counters,
        merger: inp.client.merger,
        first_from: inp.client.first_from.clone(),
        ..Report::default()
    };
    r.faults.planned = inp.plan.faults.len();
    r.faults.executed = inp.executed.len();
    r.faults.kill9 = inp
        .executed
        .iter()
        .filter(|e| e.kind == FaultKind::Kill9)
        .count();
    r.faults.freeze = r.faults.executed - r.faults.kill9;

    let invalid = inp.harness.invalid.clone().or_else(|| {
        inp.client
            .live_wall_ms
            .is_none()
            .then(|| "subscriptions never all live".to_string())
    });
    if let Some(why) = invalid {
        r.verdict = Outcome {
            result: "invalid".into(),
            reason: Some(why),
            failures: Vec::new(),
        };
        return r;
    }

    let margin = inp.plan.margin_ms;
    let t0 = inp.client.live_wall_ms.unwrap_or(0) + margin;
    let last = inp
        .executed
        .iter()
        .map(|e| e.recovered_wall_ms)
        .max()
        .unwrap_or(t0 - margin);
    let t1 = last.max(t0) + margin;
    r.window_ms = t1 - t0;
    let lo = ts_at(&inp.truth, t0);
    let hi = ts_at(&inp.truth, t1);

    let mut all_truth: HashSet<&str> = HashSet::new();
    let mut window: HashSet<&str> = HashSet::new();
    for f in &inp.truth {
        for (k, feed, ts) in &f.keys {
            all_truth.insert(k);
            if let Some(t) = ts {
                if in_window(&lo, &hi, *feed, *t) {
                    window.insert(k);
                }
            }
        }
    }
    r.truth_keys_in_window = window.len();

    let mut delivered: HashMap<&str, u32> = HashMap::new();
    for l in inp.log.iter().filter(|l| l.d) {
        *delivered.entry(&l.k).or_default() += 1;
    }
    r.missing = window
        .iter()
        .filter(|k| !delivered.contains_key(*k))
        .count() as u64;
    r.duplicates = inp
        .log
        .iter()
        .filter(|l| l.d && l.ts.is_some())
        .map(|l| l.k.as_str())
        .collect::<HashSet<_>>()
        .into_iter()
        .filter(|k| delivered[k] > 1)
        .count() as u64;
    r.extra = delivered.keys().filter(|k| !all_truth.contains(*k)).count() as u64;

    let mut late_seen: HashSet<&str> = HashSet::new();
    for l in inp.log.iter().filter(|l| !l.d) {
        if !late_seen.insert(l.k.as_str()) {
            continue;
        }
        let f = l.f as usize;
        let ts = l.ts.unwrap_or(i64::MIN);
        if delivered.contains_key(l.k.as_str()) {
            r.late.already_delivered += 1;
        } else if lo[f].is_some_and(|x| ts <= x) {
            r.late.pre_window += 1;
        } else if hi[f].is_some_and(|x| ts <= x) {
            r.late.lost += 1;
        } else {
            r.late.post_window += 1;
        }
    }

    // A4: executed as planned, and each victim seen going down.
    let as_planned = inp.executed.len() == inp.plan.faults.len()
        && inp
            .executed
            .iter()
            .zip(&inp.plan.faults)
            .all(|(e, p)| e.node == p.node && e.kind == p.kind);
    let downs: Vec<&ConnObs> = inp.client.conn.iter().filter(|c| !c.live).collect();
    let observed: Vec<Option<&ConnObs>> = inp
        .executed
        .iter()
        .map(|e| {
            downs.iter().copied().find(|c| {
                c.node == e.node
                    && c.mono_ms >= e.at_mono_ms
                    && c.mono_ms <= e.recovered_mono_ms + OBSERVE_SLACK_MS
            })
        })
        .collect();
    r.faults.observed = observed.iter().flatten().count();
    let faults_ok = as_planned && r.faults.observed == inp.executed.len();

    // A5: no vacuous pass.
    r.fault_windows_without_truth = inp
        .executed
        .iter()
        .filter(|e| {
            let (a, b) = (e.at_wall_ms * 1_000, e.recovered_wall_ms * 1_000);
            !inp.truth.iter().any(|f| f.rx_us >= a && f.rx_us <= b)
        })
        .count();
    let wanted = |n: usize| inp.client.wanted.get(n).copied().unwrap_or(true);
    let every_node_first = (0..inp.client.first_from.len())
        .filter(|&n| wanted(n))
        .all(|n| inp.client.first_from[n] > 0)
        && inp.client.first_from.len() == inp.client.wanted.len()
        && inp.client.wanted.iter().filter(|w| **w).count() >= 2;
    let vacuous = !every_node_first
        || r.fault_windows_without_truth > 0
        || window.is_empty()
        || inp.executed.is_empty();

    // Rehedge: from the victim's last message to every wanted node live.
    let mut rehedge = Vec::new();
    for c in observed.iter().flatten() {
        let from = c.last_msg_mono_ms.unwrap_or(c.mono_ms);
        let mut live = vec![false; inp.client.wanted.len()];
        let mut back = None;
        for o in &inp.client.conn {
            if o.node < live.len() {
                live[o.node] = o.live;
            }
            if o.mono_ms >= c.mono_ms
                && o.live
                && live.iter().enumerate().all(|(n, l)| *l || !wanted(n))
            {
                back = Some(o.mono_ms);
                break;
            }
        }
        if let Some(b) = back {
            rehedge.push((b - from) as f64);
        }
    }
    r.rehedge_ms = spread(rehedge);

    // Added delay: lag over the 5 s after each fault, less the delivering
    // node's own steady-state median.
    let fault_at: Vec<i64> = inp.executed.iter().map(|e| e.at_mono_ms).collect();
    let near = |m: i64| fault_at.iter().any(|&a| m >= a && m < a + DELAY_WINDOW_MS);
    let mut steady: HashMap<usize, Vec<f64>> = HashMap::new();
    let mut after: Vec<(usize, f64)> = Vec::new();
    for l in inp.log.iter().filter(|l| l.d) {
        let lag = (l.w - l.rx) as f64 / 1_000.0;
        if near(l.m) {
            after.push((l.n, lag));
        } else {
            steady.entry(l.n).or_default().push(lag);
        }
    }
    let median: HashMap<usize, f64> = steady
        .into_iter()
        .filter_map(|(n, v)| spread(v).p50.map(|m| (n, m)))
        .collect();
    r.added_delay_ms = spread(
        after
            .into_iter()
            .filter_map(|(n, lag)| median.get(&n).map(|m| lag - m))
            .collect(),
    );

    r.binance_agg_id_gaps = binance_trade_gaps(
        inp.log
            .iter()
            .filter(|l| l.d && l.ts.is_some_and(|t| in_window(&lo, &hi, l.f, t)))
            .map(|l| l.k.as_str()),
    );

    let c = &inp.client.counters;
    let checks = [
        ("seq_gap", c.seq_gaps == 0),
        ("resume", c.resume_violations == 0),
        ("unkeyed", c.unkeyed == 0),
        ("missing", r.missing == 0),
        ("duplicate", r.duplicates == 0),
        ("extra", r.extra == 0),
        ("faults_not_observed", faults_ok),
        ("vacuous", !vacuous),
    ];
    let failures: Vec<String> = checks
        .iter()
        .filter(|(_, ok)| !ok)
        .map(|(n, _)| n.to_string())
        .collect();
    r.verdict = Outcome {
        result: if failures.is_empty() { "pass" } else { "fail" }.into(),
        reason: failures.first().cloned(),
        failures,
    };
    r
}

/// The feed a log line belongs to, for tests and summaries.
pub fn feed_name(f: u8) -> &'static str {
    Feed::ALL.get(f as usize).map_or("?", |f| f.name())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A clean two-node run: truth keys at 1 ms steps from 0 to 60 s of
    /// wall time, both nodes live at 1 s, one kill of node 0 at 20 s that
    /// recovered at 22 s, and a client that delivered every key once.
    fn clean() -> Inputs {
        let base = 1_700_000_000_000i64;
        let mut truth = Vec::new();
        let mut log = Vec::new();
        for i in 0..60_000i64 {
            let ts = base + i;
            let k = format!(
                "{:?}/{:032x}/{ts}",
                KeyKind::BinanceTrade,
                (7u128 << 64) | i as u128
            );
            truth.push(TruthFrame {
                rx_us: ts * 1_000,
                keys: vec![(k.clone(), 1, Some(ts))],
            });
            if i >= 1_000 {
                log.push(LogLine {
                    k,
                    f: 1,
                    ts: Some(ts),
                    d: true,
                    n: (i % 2) as usize,
                    w: ts * 1_000 + 2_000,
                    rx: ts * 1_000 + 1_000,
                    m: i,
                });
            }
        }
        let obs = |node, live, mono_ms| ConnObs {
            node,
            live,
            wall_ms: base + mono_ms,
            mono_ms,
            last_msg_mono_ms: (!live).then_some(mono_ms - 5),
            reason: String::new(),
        };
        Inputs {
            plan: plan(1, 1, 4_000, 2_000, 15_000, None),
            executed: vec![Executed {
                node: 0,
                kind: FaultKind::Kill9,
                at_wall_ms: base + 20_000,
                at_mono_ms: 20_000,
                recovered_wall_ms: base + 22_000,
                recovered_mono_ms: 22_000,
            }],
            harness: Harness::default(),
            client: ClientRecord {
                wanted: vec![true, true],
                live_wall_ms: Some(base + 1_000),
                live_mono_ms: Some(1_000),
                conn: vec![
                    obs(0, true, 900),
                    obs(1, true, 1_000),
                    obs(0, false, 20_010),
                    obs(0, true, 22_500),
                ],
                first_from: vec![10, 10],
                ..ClientRecord::default()
            },
            log,
            truth,
        }
    }

    fn reason(i: &Inputs) -> Option<String> {
        verify(i).verdict.reason
    }

    #[test]
    fn a_clean_run_passes_with_the_window_where_the_spec_puts_it() {
        let i = clean();
        let r = verify(&i);
        assert_eq!(r.verdict.result, "pass", "{:?}", r.verdict);
        assert_eq!(r.exit_code(), 0);
        // T0 = 1 s + 15 s, T1 = 22 s + 15 s.
        assert_eq!(r.window_ms, 21_000);
        assert_eq!(r.truth_keys_in_window, 21_000);
        assert_eq!(r.rehedge_ms.max, Some(2_495.0));
        assert_eq!(r.added_delay_ms.max, Some(0.0));
        assert_eq!(r.binance_agg_id_gaps, 0);
        assert_eq!(r.faults.observed, 1);
    }

    #[test]
    fn each_check_fails_by_its_own_name() {
        // A1: one key inside W never delivered.
        let mut i = clean();
        i.log.retain(|l| l.ts != Some(1_700_000_030_000));
        let r = verify(&i);
        assert_eq!(r.verdict.reason.as_deref(), Some("missing"));
        assert_eq!(r.missing, 1);
        assert_eq!(r.binance_agg_id_gaps, 1);
        // The same key outside W is not missing.
        let mut i = clean();
        i.log.retain(|l| l.ts != Some(1_700_000_005_000));
        assert_eq!(reason(&i), None);

        // A2: any timed key twice, even outside W.
        let mut i = clean();
        let again = i.log[10].clone();
        i.log.push(again);
        assert_eq!(reason(&i).as_deref(), Some("duplicate"));

        // A3: a delivered key the truth never had.
        let mut i = clean();
        let mut x = i.log[10].clone();
        x.k.push('x');
        i.log.push(x);
        assert_eq!(reason(&i).as_deref(), Some("extra"));

        // A4: a fault not executed, one of another kind, one not seen.
        let mut i = clean();
        i.executed.clear();
        assert_eq!(reason(&i).as_deref(), Some("faults_not_observed"));
        let mut i = clean();
        i.executed[0].kind = FaultKind::Freeze;
        assert_eq!(reason(&i).as_deref(), Some("faults_not_observed"));
        let mut i = clean();
        i.client.conn.retain(|c| c.live);
        assert_eq!(reason(&i).as_deref(), Some("faults_not_observed"));

        // A5: one node never first, or no truth inside a fault.
        let mut i = clean();
        i.client.first_from[1] = 0;
        assert_eq!(reason(&i).as_deref(), Some("vacuous"));
        let mut i = clean();
        i.truth
            .retain(|f| !(f.rx_us >= 1_700_000_019_000_000 && f.rx_us <= 1_700_000_023_000_000));
        let r = verify(&i);
        assert_eq!(r.fault_windows_without_truth, 1);
        assert!(r.verdict.failures.contains(&"vacuous".to_string()));

        // A6 counters, each by name, and first in the order.
        let cases: [(&str, fn(&mut Counters)); 3] = [
            ("seq_gap", |c| c.seq_gaps = 1),
            ("resume", |c| c.resume_violations = 1),
            ("unkeyed", |c| c.unkeyed = 1),
        ];
        for (name, set) in cases {
            let mut i = clean();
            set(&mut i.client.counters);
            i.log.retain(|l| l.ts != Some(1_700_000_030_000));
            let r = verify(&i);
            assert_eq!(r.verdict.reason.as_deref(), Some(name));
            assert_eq!(r.verdict.failures, vec![name.to_string(), "missing".into()]);
        }

        // A7: the harness says the run cannot count.
        let mut i = clean();
        i.harness.invalid = Some("two nodes down".into());
        let r = verify(&i);
        assert_eq!((r.verdict.result.as_str(), r.exit_code()), ("invalid", 2));
        let mut i = clean();
        i.client.live_wall_ms = None;
        assert_eq!(verify(&i).exit_code(), 2);
    }

    #[test]
    fn late_keys_are_classed_against_the_delivered_log_and_the_window() {
        let mut i = clean();
        let mk = |ts: i64, i: &Inputs| {
            let mut l = i
                .log
                .iter()
                .find(|l| l.ts == Some(ts))
                .cloned()
                .unwrap_or_else(|| {
                    let mut l = i.log[0].clone();
                    l.ts = Some(ts);
                    l.k = format!("{:?}/{:032x}/{ts}", KeyKind::BinanceTrade, 9u128);
                    l
                });
            l.d = false;
            l
        };
        // Already delivered.
        let a = mk(1_700_000_010_000, &i);
        // Lost inside W: drop its delivery.
        let b = mk(1_700_000_030_000, &i);
        i.log.retain(|l| l.ts != Some(1_700_000_030_000));
        // Before the window, never delivered.
        let c = mk(1_700_000_000_500, &i);
        i.log.extend([a, b, c]);
        let r = verify(&i);
        assert_eq!(
            r.late,
            Late {
                already_delivered: 1,
                pre_window: 1,
                lost: 1,
                post_window: 0
            }
        );
        // A lost late key is also missing.
        assert_eq!(r.missing, 1);
    }

    #[test]
    fn a_recording_window_keeps_keys_broadcast_before_t0_out() {
        // Venue timestamps a day older than the broadcast time.
        let mut i = clean();
        let day = 86_400_000i64;
        for f in &mut i.truth {
            for k in &mut f.keys {
                k.2 = k.2.map(|t| t - day);
            }
        }
        for l in &mut i.log {
            l.ts = l.ts.map(|t| t - day);
        }
        // Keys are text and still match; only the window moves.
        let r = verify(&i);
        assert_eq!(r.verdict.result, "pass", "{:?}", r.verdict);
        assert_eq!(r.truth_keys_in_window, 21_000);
    }

    #[test]
    fn the_plan_alternates_nodes_and_kinds_and_overlap_is_all_kills() {
        let p = plan(3, 8, 4_000, 2_000, 15_000, None);
        let shape: Vec<(usize, FaultKind)> = p.faults.iter().map(|f| (f.node, f.kind)).collect();
        use FaultKind::*;
        assert_eq!(
            shape,
            vec![
                (0, Kill9),
                (1, Freeze),
                (0, Freeze),
                (1, Kill9),
                (0, Kill9),
                (1, Freeze),
                (0, Freeze),
                (1, Kill9)
            ]
        );
        assert!(p
            .faults
            .windows(2)
            .all(|w| w[1].at_ms - w[0].at_ms >= 3_000));
        assert_eq!(p, plan(3, 8, 4_000, 2_000, 15_000, None));
        assert_ne!(p.faults, plan(4, 8, 4_000, 2_000, 15_000, None).faults);
        let o = plan(3, 2, 4_000, 2_000, 15_000, Some(OVERLAP_KILL));
        assert!(o.overlap);
        assert!(o.faults.iter().all(|f| f.kind == Kill9));
        assert_eq!(o.faults[1].at_ms - o.faults[0].at_ms, 300);
    }

    #[test]
    fn spread_and_trade_gaps() {
        let s = spread((1..=100).map(f64::from).collect());
        assert_eq!((s.p50, s.p99, s.max), (Some(51.0), Some(99.0), Some(100.0)));
        assert_eq!(spread(Vec::new()).p50, None);
        let k =
            |sym: u128, a: u128| format!("{:?}/{:032x}/1", KeyKind::BinanceTrade, (sym << 64) | a);
        let keys = [k(1, 5), k(1, 6), k(1, 8), k(2, 1), k(2, 2), k(1, 6)];
        assert_eq!(binance_trade_gaps(keys.iter().map(|s| s.as_str())), 1);
        assert_eq!(feed_name(2), "clob");
    }
}
