//! The live drill on Azure (spec 4.7): its fault plan, the truth it is
//! judged against, and its verdict. Process control lives in `bin/chaos`
//! (`chaos drill`, `chaos verify-drill`); the method is in
//! `docs/HA_DRILL.md`.
//!
//! The drill reuses the CI harness. The client, the fault driver and the
//! stream verdict ([`crate::chaos::verify`]) are the same code; what
//! changes is where the nodes run, what truth is, and the archive check.
//!
//! - **Truth** is the union of both nodes' own recordings (their local
//!   segment files for the drill window, gzipped by `drill-collect.sh`),
//!   not a simulator's manifest. An event both nodes missed is invisible to
//!   it. `extra` (delivered keys in neither recording) is reported, not
//!   gated: a killed node can forward a frame and lose the last second of
//!   its file.
//! - **Content**: a key both nodes recorded in a frame of its own must carry
//!   the same frame text on both. Counted per feed. Binance is gated at 0;
//!   RTDS and CLOB are reported (see the deviation in the spec, section 9).
//! - **Control**: a second client subscribed to node-a only. Its `missing`
//!   over the same window must be above 0, or the faults did not bite and
//!   the run is invalid.
//! - **Archive**: there is no journal of HEAD writes on Blob, so the audit
//!   reads the final HEAD, the list of archive objects and both nodes'
//!   `lease.jsonl`, limited to the drill window ([`verify_archive_drill`]).
//!
//! Run directory, in addition to the files of [`crate::chaos`]:
//!
//! | File | What |
//! | --- | --- |
//! | `times.json` | wall times the drill started and ended, for collection |
//! | `control/client.json`, `control/log.jsonl` | the single-node client |
//! | `stream.json` | the stream verdict alone |
//! | `report.json` | the drill verdict: counts and timings only |

use crate::chaos::{self, ClientRecord, Executed, FaultKind, LogLine, Outcome, Plan, PlannedFault};
use crate::chaos::{Report, Spread, TruthFrame};
use ha::caslease::{self, Head, ObjectRole};
use ha::key::frame_keys;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

pub const TIMES: &str = "times.json";
pub const CONTROL: &str = "control";
pub const STREAM: &str = "stream.json";

/// The spec's gate on a takeover after a leader freeze.
pub const TAKEOVER_GATE_MS: i64 = 45_000;

/// When the drill ran, on the runner's wall clock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Times {
    pub start_ms: i64,
    pub end_ms: i64,
}

/// `faults` faults `gap_ms` apart, `leader_freezes` of them leader freezes
/// of `freeze_ms`, spread evenly through the run, the rest `kill9`
/// alternating between the nodes from node-a. Fault i is due
/// `1 s + i * gap` after T0 plus a seeded jitter below a quarter gap, as in
/// [`chaos::plan`].
pub fn drill_plan(
    seed: u64,
    faults: usize,
    gap_ms: i64,
    leader_freezes: usize,
    freeze_ms: u64,
    margin_ms: i64,
) -> Plan {
    let freezes = leader_freezes.min(faults);
    let base = chaos::plan(seed, faults, gap_ms, freeze_ms, margin_ms, None);
    let mut kills = 0usize;
    let faults = base
        .faults
        .iter()
        .enumerate()
        .map(|(i, f)| {
            // Fault i is a freeze when (i + 1) * L / F passes an integer.
            let freeze = faults > 0 && (i + 1) * freezes / faults > i * freezes / faults;
            if freeze {
                PlannedFault {
                    node: 0,
                    kind: FaultKind::FreezeLeader,
                    at_ms: f.at_ms,
                    freeze_ms,
                }
            } else {
                let node = kills % 2;
                kills += 1;
                PlannedFault {
                    node,
                    kind: FaultKind::Kill9,
                    at_ms: f.at_ms,
                    freeze_ms: 0,
                }
            }
        })
        .collect();
    Plan { faults, ..base }
}

/// Same key, different frame text on the two nodes, per feed. Only keys
/// that are alone in their frame on both nodes are compared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Content {
    pub compared: [u64; 3],
    pub mismatch: [u64; 3],
}

/// Truth built from the nodes' recordings.
pub struct Union {
    pub frames: Vec<TruthFrame>,
    pub content: Content,
    /// Frames read per node.
    pub node_frames: Vec<u64>,
    /// Files whose last record was torn (a killed writer).
    pub torn_files: u64,
}

fn hash_of<T: Hash + ?Sized>(t: &T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

/// Every recording file under `dir`, in name order: `.prec`, `.prec.open`
/// and either of those gzipped.
fn recording_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            recording_files(&p, out);
            continue;
        }
        let name = p.file_name().map(|n| n.to_string_lossy().into_owned());
        let name = name.as_deref().unwrap_or("");
        let base = name.strip_suffix(".gz").unwrap_or(name);
        if base.ends_with(".prec") || base.ends_with(".prec.open") {
            out.push(p);
        }
    }
}

/// Read every record of one file; a torn tail ends it. Returns whether it
/// was torn.
fn read_records(path: &Path, mut each: impl FnMut(feeds::recording::Record)) -> io::Result<bool> {
    let f = std::fs::File::open(path)?;
    let input: Box<dyn Read> = if path.extension().is_some_and(|e| e == "gz") {
        Box::new(flate2::read::GzDecoder::new(f))
    } else {
        Box::new(f)
    };
    let Ok(rr) = feeds::recording::RecordReader::new(io::BufReader::new(input)) else {
        // An empty or headless file: a writer killed at once.
        return Ok(true);
    };
    for rec in rr {
        match rec {
            Ok(r) => each(r),
            Err(_) => return Ok(true),
        }
    }
    Ok(false)
}

/// The union of the recordings under each of `nodes` (one directory per
/// node), with the content comparison between the first two.
pub fn load_union(nodes: &[PathBuf]) -> io::Result<Union> {
    let mut u = Union {
        frames: Vec::new(),
        content: Content::default(),
        node_frames: vec![0; nodes.len()],
        torn_files: 0,
    };
    // Key hash -> frame hash, for single-key frames of the first node.
    let mut first: HashMap<u64, (u64, u8)> = HashMap::new();
    let mut seen_second: HashSet<u64> = HashSet::new();
    let mut keys = Vec::new();
    for (i, dir) in nodes.iter().enumerate() {
        let mut files = Vec::new();
        recording_files(dir, &mut files);
        for path in files {
            let torn = read_records(&path, |r| {
                keys.clear();
                frame_keys(r.source, &r.frame, &mut keys);
                if keys.is_empty() {
                    return;
                }
                u.node_frames[i] += 1;
                let texts: Vec<(String, u8, Option<i64>)> = keys
                    .iter()
                    .map(|k| (chaos::key_text(k), k.kind.feed().index() as u8, k.ts_millis))
                    .collect();
                if texts.len() == 1 {
                    let (k, feed, _) = &texts[0];
                    let kh = hash_of(k.as_str());
                    let fh = hash_of(r.frame.as_str());
                    match i {
                        0 => {
                            first.insert(kh, (fh, *feed));
                        }
                        1 => {
                            if let Some((h0, _)) = first.get(&kh) {
                                if seen_second.insert(kh) {
                                    u.content.compared[*feed as usize] += 1;
                                    if *h0 != fh {
                                        u.content.mismatch[*feed as usize] += 1;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                u.frames.push(TruthFrame {
                    rx_us: r.recv_micros,
                    keys: texts,
                });
            })?;
            if torn {
                u.torn_files += 1;
            }
        }
    }
    u.frames.sort_by_key(|f| f.rx_us);
    Ok(u)
}

/// One line of a node's `lease.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseLine {
    pub wall_ms: i64,
    pub node: u32,
    pub ev: String,
    pub epoch: u64,
    #[serde(default)]
    pub slot: Option<i64>,
}

/// The reader rule over the drill window's slots only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowView {
    pub first_slot: i64,
    pub last_slot: i64,
    pub covered: u64,
    pub archived: u64,
    pub holes: u64,
    pub overlaps: u64,
    pub orphans: u64,
    pub ambiguous: u64,
}

/// The archive verdict of a drill.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DrillArchive {
    pub verdict: Outcome,
    pub head_read: bool,
    pub final_epoch: u64,
    pub view: WindowView,
    /// Grant lines in the window, both nodes.
    pub grants: u64,
    pub commits: u64,
    /// `fenced` lines in the window, both nodes.
    pub fence_rejected: u64,
    /// Commits logged under an epoch that HEAD does not hold for that slot,
    /// or after a higher epoch was granted.
    pub stale_epoch_commits: u64,
    /// Epochs granted to two nodes, in the logs or in HEAD.
    pub epoch_two_holders: u64,
    /// HEAD tenures out of order, overlapping or not contiguous.
    pub tenure_violations: u64,
    pub leader_freezes: usize,
    pub takeovers_seen: usize,
    pub takeover_ms: Spread,
    pub takeover_gate_ms: i64,
}

/// Lines written within this much of a higher grant on the other node's
/// clock are not counted as after it: the two VM clocks are not one clock.
const SKEW_MS: i64 = 2_000;

/// The archive verdict from the final HEAD, the archive object names, the
/// nodes' lease logs and the executed faults, over the slots between
/// `times.start_ms` and two slots before `times.end_ms` (a slot is
/// committed once it is two slots old). Anything older is outside the
/// drill, and slots older than the lifecycle's two days may have lost
/// their objects.
pub fn verify_archive_drill(
    head: Option<&Head>,
    objects: &[String],
    lease: &[LeaseLine],
    executed: &[Executed],
    times: Times,
    slot_ms: i64,
    takeover_gate_ms: i64,
) -> DrillArchive {
    let mut r = DrillArchive {
        head_read: head.is_some(),
        takeover_gate_ms,
        ..DrillArchive::default()
    };
    let in_time = |l: &&LeaseLine| l.wall_ms >= times.start_ms && l.wall_ms <= times.end_ms;
    let window: Vec<&LeaseLine> = lease.iter().filter(in_time).collect();
    r.grants = window.iter().filter(|l| l.ev == "grant").count() as u64;
    r.commits = window.iter().filter(|l| l.ev == "commit").count() as u64;
    r.fence_rejected = window.iter().filter(|l| l.ev == "fenced").count() as u64;

    // Each epoch is granted to one node: in the logs (whole files, since a
    // grant just before the window still holds in it) and in HEAD.
    let mut holders: HashMap<u64, HashSet<u32>> = HashMap::new();
    for l in lease.iter().filter(|l| l.ev == "grant") {
        holders.entry(l.epoch).or_default().insert(l.node);
    }
    if let Some(h) = head {
        for t in &h.tenures {
            holders.entry(t.e).or_default().insert(t.n);
        }
    }
    r.epoch_two_holders = holders.values().filter(|s| s.len() > 1).count() as u64;

    // No commit under an epoch once a higher one was granted, on the
    // grant's clock plus the skew allowance.
    let grants: Vec<(u64, i64)> = lease
        .iter()
        .filter(|l| l.ev == "grant")
        .map(|l| (l.epoch, l.wall_ms))
        .collect();
    let after_higher = |c: &LeaseLine| {
        grants
            .iter()
            .any(|&(e, w)| e > c.epoch && c.wall_ms > w + SKEW_MS)
    };
    let mut stale = 0u64;
    for c in window.iter().filter(|l| l.ev == "commit") {
        let held = match (head, c.slot) {
            (Some(h), Some(s)) => h
                .tenures
                .iter()
                .any(|t| t.e == c.epoch && t.from <= s && s <= t.to),
            // Trimmed or unreadable: judged by the logs alone.
            _ => true,
        };
        let trimmed = match (head, c.slot) {
            (Some(h), Some(s)) => h.tenures.first().is_some_and(|t| s < t.from),
            _ => false,
        };
        if (!held && !trimmed) || after_higher(c) {
            stale += 1;
        }
    }
    r.stale_epoch_commits = stale;

    let first_slot = times.start_ms.div_euclid(slot_ms);
    let mut last_slot = times.end_ms.div_euclid(slot_ms) - 2;
    if let Some(h) = head {
        r.final_epoch = h.epoch;
        let mut prev: Option<&caslease::Tenure> = None;
        for t in &h.tenures {
            let bad_order = prev.is_some_and(|p| t.e <= p.e || t.from != p.to + 1);
            if t.from > t.to || bad_order {
                r.tenure_violations += 1;
            }
            prev = Some(t);
        }
        if let Some(c) = h.committed_to() {
            last_slot = last_slot.min(c);
        }
        r.view = window_view(h, objects, first_slot, last_slot);
    } else {
        r.view.first_slot = first_slot;
        r.view.last_slot = last_slot;
    }

    let freezes: Vec<&Executed> = executed
        .iter()
        .filter(|e| e.kind == FaultKind::FreezeLeader)
        .collect();
    r.leader_freezes = freezes.len();
    let takeovers: Vec<f64> = freezes
        .iter()
        .filter_map(|e| e.takeover_ms.map(|t| t as f64))
        .collect();
    r.takeovers_seen = takeovers.len();
    r.takeover_ms = chaos::spread_of(takeovers);
    let slow = r
        .takeover_ms
        .max
        .is_some_and(|m| m > takeover_gate_ms as f64);

    let checks = [
        ("head_unreadable", r.head_read),
        ("stale_epoch_commit", r.stale_epoch_commits == 0),
        ("epoch_two_holders", r.epoch_two_holders == 0),
        ("tenure", r.tenure_violations == 0 && r.view.ambiguous == 0),
        ("no_takeover", r.takeovers_seen == r.leader_freezes),
        ("takeover_slow", !slow),
        (
            "fence_not_observed",
            r.fence_rejected >= r.leader_freezes as u64,
        ),
        (
            "vacuous_archive",
            r.view.archived > 0 && r.grants >= r.leader_freezes as u64,
        ),
    ];
    r.verdict = chaos::outcome_of(&checks);
    r
}

fn window_view(h: &Head, objects: &[String], lo: i64, hi: i64) -> WindowView {
    let mut v = WindowView {
        first_slot: lo,
        last_slot: hi,
        ..WindowView::default()
    };
    let present: HashSet<(u64, i64)> = objects
        .iter()
        .filter_map(|n| caslease::parse_object_name(n))
        .collect();
    for s in lo..=hi {
        let es: Vec<u64> = h
            .tenures
            .iter()
            .filter(|t| t.from <= s && s <= t.to)
            .map(|t| t.e)
            .collect();
        match es.as_slice() {
            [] => {}
            [e] => {
                v.covered += 1;
                if present.contains(&(*e, s)) {
                    v.archived += 1;
                } else {
                    v.holes += 1;
                }
            }
            _ => v.ambiguous += 1,
        }
    }
    for &(e, s) in present.iter().filter(|(_, s)| (lo..=hi).contains(s)) {
        match caslease::object_role(h, e, s) {
            ObjectRole::Archived => {}
            ObjectRole::Overlap => v.overlaps += 1,
            ObjectRole::Orphan => v.orphans += 1,
        }
    }
    v
}

/// Truth sizes, for the report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TruthSummary {
    pub node_frames: Vec<u64>,
    pub torn_files: u64,
}

/// The drill verdict. Counts and timings only: this is the public artifact.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DrillReport {
    pub verdict: Outcome,
    pub stream: Report,
    /// Window keys the node-a-only client never delivered.
    pub control_missing: u64,
    pub content: Content,
    pub truth: TruthSummary,
    pub archive: DrillArchive,
}

impl DrillReport {
    pub fn exit_code(&self) -> i32 {
        match self.verdict.result.as_str() {
            "pass" => 0,
            "invalid" => 2,
            _ => 1,
        }
    }
}

/// Everything the drill verdict is computed from.
pub struct DrillInputs {
    pub stream: chaos::Inputs,
    pub control: Option<(ClientRecord, Vec<LogLine>)>,
    pub content: Content,
    pub truth: TruthSummary,
    pub head: Option<Head>,
    pub objects: Vec<String>,
    pub lease: Vec<LeaseLine>,
    pub times: Times,
}

/// Read a run directory and the collected directory: `n1/` and `n2/` each
/// with the node's segments (any depth) and `lease.jsonl`, plus
/// `head.json` and `objects.txt` (one archive object name a line).
pub fn load(run: &Path, collected: &Path) -> io::Result<DrillInputs> {
    let nodes = [collected.join("n1"), collected.join("n2")];
    for n in &nodes {
        if !n.is_dir() {
            return Err(io::Error::other(format!("{n:?} is missing")));
        }
    }
    let union = load_union(&nodes)?;
    let stream = chaos::Inputs {
        plan: chaos::read_json(&run.join(chaos::PLAN))?,
        executed: if run.join(chaos::FAULTS).exists() {
            chaos::read_jsonl(&run.join(chaos::FAULTS))?
        } else {
            Vec::new()
        },
        harness: if run.join(chaos::HARNESS).exists() {
            chaos::read_json(&run.join(chaos::HARNESS))?
        } else {
            chaos::Harness::default()
        },
        client: chaos::read_json(&run.join(chaos::CLIENT))?,
        log: chaos::read_jsonl(&run.join(chaos::LOG))?,
        truth: union.frames,
    };
    let cdir = run.join(CONTROL);
    let control = if cdir.join(chaos::CLIENT).exists() {
        Some((
            chaos::read_json(&cdir.join(chaos::CLIENT))?,
            chaos::read_jsonl(&cdir.join(chaos::LOG))?,
        ))
    } else {
        None
    };
    let head = std::fs::read(collected.join("head.json"))
        .ok()
        .and_then(|b| Head::decode(b.trim_ascii()));
    let objects = std::fs::read_to_string(collected.join("objects.txt"))
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    let mut lease = Vec::new();
    for n in &nodes {
        if let Ok(text) = std::fs::read_to_string(n.join("lease.jsonl")) {
            lease.extend(
                text.lines()
                    .filter_map(|l| serde_json::from_str::<LeaseLine>(l).ok()),
            );
        }
    }
    Ok(DrillInputs {
        stream,
        control,
        content: union.content,
        truth: TruthSummary {
            node_frames: union.node_frames,
            torn_files: union.torn_files,
        },
        head,
        objects,
        lease,
        times: chaos::read_json(&run.join(TIMES))?,
    })
}

/// Drop one gated check from an outcome: it stays reported elsewhere.
fn ungate(o: &mut Outcome, name: &str) {
    o.failures.retain(|f| f != name);
    o.reason = o.failures.first().cloned();
    if o.result == "fail" && o.failures.is_empty() {
        o.result = "pass".into();
    }
}

pub fn verify(inp: &mut DrillInputs, slot_ms: i64, takeover_gate_ms: i64) -> DrillReport {
    let mut stream = chaos::verify(&inp.stream);
    // A killed node can forward a frame and lose the last second of its
    // file, so a delivered key in neither recording is not a failure here.
    ungate(&mut stream.verdict, "extra");

    // The control client over the main client's window: same T0, same
    // faults, its own log.
    let mut control_missing = 0;
    if let Some((rec, log)) = inp.control.take() {
        let main_live = inp.stream.client.live_wall_ms;
        let mut rec = rec;
        rec.live_wall_ms = main_live;
        let main_client = std::mem::replace(&mut inp.stream.client, rec);
        let main_log = std::mem::replace(&mut inp.stream.log, log);
        control_missing = chaos::verify(&inp.stream).missing;
        let rec = std::mem::replace(&mut inp.stream.client, main_client);
        let log = std::mem::replace(&mut inp.stream.log, main_log);
        inp.control = Some((rec, log));
    }

    let archive = verify_archive_drill(
        inp.head.as_ref(),
        &inp.objects,
        &inp.lease,
        &inp.stream.executed,
        inp.times,
        slot_ms,
        takeover_gate_ms,
    );

    let node_a_kills = inp
        .stream
        .executed
        .iter()
        .filter(|e| e.kind == FaultKind::Kill9 && e.node == 0)
        .count();
    let invalid = if stream.verdict.result == "invalid" {
        stream.verdict.reason.clone()
    } else if stream.faults.executed != stream.faults.planned {
        Some(format!(
            "executed {} of {} planned faults",
            stream.faults.executed, stream.faults.planned
        ))
    } else if inp.control.is_none() {
        Some("no control client".into())
    } else if node_a_kills > 0 && control_missing == 0 {
        Some("the node-a-only control client missed nothing".into())
    } else {
        None
    };

    let binance = ha::key::Feed::Binance.index();
    let mut failures = stream.verdict.failures.clone();
    if inp.content.mismatch[binance] > 0 {
        failures.push("content_mismatch".into());
    }
    failures.extend(archive.verdict.failures.iter().cloned());
    let verdict = match invalid {
        Some(why) => Outcome {
            result: "invalid".into(),
            reason: Some(why),
            failures,
        },
        None => Outcome {
            result: if failures.is_empty() { "pass" } else { "fail" }.into(),
            reason: failures.first().cloned(),
            failures,
        },
    };
    DrillReport {
        verdict,
        stream,
        control_missing,
        content: inp.content,
        truth: inp.truth.clone(),
        archive,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use caslease::Tenure;

    #[test]
    fn the_drill_plan_spreads_leader_freezes_and_alternates_kills() {
        let p = drill_plan(9, 20, 90_000, 6, 60_000, 15_000);
        assert_eq!(p.faults.len(), 20);
        let freezes: Vec<usize> = p
            .faults
            .iter()
            .enumerate()
            .filter(|(_, f)| f.kind == FaultKind::FreezeLeader)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(freezes.len(), 6);
        // Spread out, not bunched at the end.
        assert!(freezes[0] < 5 && freezes[5] == 19, "{freezes:?}");
        let kills: Vec<usize> = p
            .faults
            .iter()
            .filter(|f| f.kind == FaultKind::Kill9)
            .map(|f| f.node)
            .collect();
        assert_eq!(kills.len(), 14);
        assert!(kills.iter().enumerate().all(|(i, n)| *n == i % 2));
        assert!(p
            .faults
            .iter()
            .all(|f| f.kind != FaultKind::FreezeLeader || f.freeze_ms == 60_000));
        for w in p.faults.windows(2) {
            assert!(w[1].at_ms - w[0].at_ms > 90_000 * 3 / 4);
        }
        // The rehearsal: three kills (a, b, a) and one leader freeze.
        let r = drill_plan(9, 4, 90_000, 1, 60_000, 15_000);
        let kinds: Vec<(FaultKind, usize)> = r.faults.iter().map(|f| (f.kind, f.node)).collect();
        assert_eq!(
            kinds,
            vec![
                (FaultKind::Kill9, 0),
                (FaultKind::Kill9, 1),
                (FaultKind::Kill9, 0),
                (FaultKind::FreezeLeader, 0)
            ]
        );
    }

    fn line(wall_ms: i64, node: u32, ev: &str, epoch: u64, slot: Option<i64>) -> LeaseLine {
        LeaseLine {
            wall_ms,
            node,
            ev: ev.into(),
            epoch,
            slot,
        }
    }

    fn head(t: &[(u64, u32, i64, i64)]) -> Head {
        Head {
            v: 1,
            epoch: t.last().map_or(1, |x| x.0),
            holder: t.last().map(|x| x.1),
            ttl_ms: 30_000,
            nonce: "n".into(),
            tenures: t
                .iter()
                .map(|&(e, n, from, to)| Tenure { e, n, from, to })
                .collect(),
        }
    }

    fn freeze(takeover_ms: Option<i64>) -> Executed {
        Executed {
            node: 0,
            kind: FaultKind::FreezeLeader,
            at_wall_ms: 0,
            at_mono_ms: 0,
            recovered_wall_ms: 0,
            recovered_mono_ms: 0,
            takeover_ms,
        }
    }

    /// Slots of one minute. The drill runs over slots 100 to 120; epoch 2
    /// (node 1) holds 90..=105, epoch 3 (node 2) 106..=118 after a leader
    /// freeze, and node 1 was fenced once.
    fn clean() -> (Head, Vec<String>, Vec<LeaseLine>, Vec<Executed>, Times) {
        let h = head(&[(2, 1, 90, 105), (3, 2, 106, 118)]);
        let mut objects = Vec::new();
        for s in 90..=105 {
            objects.push(caslease::object_name(2, s));
        }
        objects.push(caslease::object_name(3, 105));
        for s in 106..=118 {
            objects.push(caslease::object_name(3, s));
        }
        let m = 60_000;
        let mut lease = vec![line(80 * m, 1, "grant", 2, None)];
        for s in 98..=105 {
            lease.push(line((s + 2) * m, 1, "commit", 2, Some(s)));
        }
        lease.push(line(107 * m + 40_000, 2, "grant", 3, None));
        lease.push(line(108 * m + 1_000, 1, "fenced", 2, None));
        for s in 106..=118 {
            lease.push(line((s + 2) * m + 5_000, 2, "commit", 3, Some(s)));
        }
        let times = Times {
            start_ms: 100 * m,
            end_ms: 121 * m,
        };
        (h, objects, lease, vec![freeze(Some(36_000))], times)
    }

    fn reason(
        h: Option<&Head>,
        o: &[String],
        l: &[LeaseLine],
        e: &[Executed],
        t: Times,
    ) -> Option<String> {
        verify_archive_drill(h, o, l, e, t, 60_000, TAKEOVER_GATE_MS)
            .verdict
            .reason
    }

    #[test]
    fn a_clean_drill_archive_passes_over_its_own_window() {
        let (h, o, l, e, t) = clean();
        let r = verify_archive_drill(Some(&h), &o, &l, &e, t, 60_000, TAKEOVER_GATE_MS);
        assert_eq!(r.verdict.result, "pass", "{:?}", r.verdict);
        // Slots 100 to 119 (end slot 121 minus two), cut at the committed 118.
        assert_eq!((r.view.first_slot, r.view.last_slot), (100, 118));
        assert_eq!(r.view.covered, 19);
        assert_eq!(r.view.archived, 19);
        assert_eq!(r.view.holes, 0);
        assert_eq!(r.view.overlaps, 1);
        assert_eq!(r.view.orphans, 0);
        assert_eq!(r.grants, 1);
        assert_eq!(r.fence_rejected, 1);
        assert_eq!(r.takeover_ms.max, Some(36_000.0));
    }

    #[test]
    fn each_drill_archive_check_fails_by_its_own_name() {
        let (h, o, l, e, t) = clean();
        let m = 60_000;

        // A zombie's commit under epoch 2 after epoch 3 was granted.
        let mut l2 = l.clone();
        l2.push(line(109 * m, 1, "commit", 2, Some(107)));
        assert_eq!(
            reason(Some(&h), &o, &l2, &e, t).as_deref(),
            Some("stale_epoch_commit")
        );

        // Epoch 3 granted to both nodes.
        let mut l3 = l.clone();
        l3.push(line(107 * m + 41_000, 1, "grant", 3, None));
        assert_eq!(
            reason(Some(&h), &o, &l3, &e, t).as_deref(),
            Some("epoch_two_holders")
        );

        // A tenure that does not follow its predecessor.
        let gap = head(&[(2, 1, 90, 105), (3, 2, 107, 118)]);
        let l4: Vec<LeaseLine> = l.iter().filter(|x| x.slot != Some(106)).cloned().collect();
        assert_eq!(
            reason(Some(&gap), &o, &l4, &e, t).as_deref(),
            Some("tenure")
        );

        assert_eq!(
            reason(Some(&h), &o, &l, &[freeze(None)], t).as_deref(),
            Some("no_takeover")
        );
        assert_eq!(
            reason(Some(&h), &o, &l, &[freeze(Some(46_000))], t).as_deref(),
            Some("takeover_slow")
        );

        let unfenced: Vec<LeaseLine> = l.iter().filter(|x| x.ev != "fenced").cloned().collect();
        assert_eq!(
            reason(Some(&h), &o, &unfenced, &e, t).as_deref(),
            Some("fence_not_observed")
        );

        assert_eq!(
            reason(Some(&h), &[], &l, &e, t).as_deref(),
            Some("vacuous_archive")
        );
        assert_eq!(
            reason(None, &o, &l, &e, t).as_deref(),
            Some("head_unreadable")
        );
    }

    #[test]
    fn holes_and_orphans_are_counted_inside_the_window_only() {
        let (h, mut o, l, e, t) = clean();
        // Slot 95 is outside the window: its loss is not counted.
        o.retain(|n| n != &caslease::object_name(2, 95) && n != &caslease::object_name(2, 101));
        // A fenced writer's object for slot 110.
        o.push(caslease::object_name(2, 110));
        let r = verify_archive_drill(Some(&h), &o, &l, &e, t, 60_000, TAKEOVER_GATE_MS);
        assert_eq!(r.verdict.result, "pass", "{:?}", r.verdict);
        assert_eq!(r.view.holes, 1);
        assert_eq!(r.view.orphans, 1);
    }

    #[test]
    fn ungating_extra_keeps_the_other_failures() {
        let mut o = Outcome {
            result: "fail".into(),
            reason: Some("extra".into()),
            failures: vec!["extra".into()],
        };
        ungate(&mut o, "extra");
        assert_eq!(o.result, "pass");
        assert_eq!(o.reason, None);
        let mut o = Outcome {
            result: "fail".into(),
            reason: Some("missing".into()),
            failures: vec!["missing".into(), "extra".into()],
        };
        ungate(&mut o, "extra");
        assert_eq!(o.result, "fail");
        assert_eq!(o.reason.as_deref(), Some("missing"));
    }

    fn write_prec(path: &Path, frames: &[(i64, feeds::recording::Source, &str)], torn: bool) {
        let f = std::fs::File::create(path).unwrap();
        let mut w = feeds::recording::RecordWriter::new(io::BufWriter::new(f)).unwrap();
        for (rx, src, frame) in frames {
            w.append(*rx, *src, frame).unwrap();
        }
        let mut inner = w.into_inner().unwrap();
        if torn {
            use std::io::Write;
            inner.write_all(&[7, 0, 0]).unwrap();
        }
        std::io::Write::flush(&mut inner).unwrap();
    }

    #[test]
    fn union_truth_reads_plain_and_gzipped_files_and_compares_content() {
        use feeds::recording::Source;
        let dir = std::env::temp_dir().join(format!("poly-drill-union-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (n1, n2) = (dir.join("n1/seg"), dir.join("n2/seg"));
        std::fs::create_dir_all(&n1).unwrap();
        std::fs::create_dir_all(&n2).unwrap();
        let trade = |id: u64, price: &str| {
            format!(
                r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":1700000000{id:03},"s":"BTCUSDT","a":{id},"p":"{price}","q":"1","f":1,"l":1,"T":1700000000{id:03},"m":false}}}}"#
            )
        };
        let (t1, t2, t3, t3b) = (
            trade(1, "10"),
            trade(2, "11"),
            trade(3, "12"),
            trade(3, "99"),
        );
        write_prec(
            &n1.join("100.aa.prec"),
            &[(1, Source::Binance, &t1), (2, Source::Binance, &t2)],
            false,
        );
        // Node 2: gzipped, with key 3 under another price, and a torn tail.
        let plain = n2.join("100.bb.prec.open");
        write_prec(
            &plain,
            &[(3, Source::Binance, &t2), (4, Source::Binance, &t3b)],
            true,
        );
        let gz = n2.join("100.bb.prec.open.gz");
        {
            let mut e = flate2::write::GzEncoder::new(
                std::fs::File::create(&gz).unwrap(),
                flate2::Compression::fast(),
            );
            std::io::copy(&mut std::fs::File::open(&plain).unwrap(), &mut e).unwrap();
            e.finish().unwrap();
        }
        std::fs::remove_file(&plain).unwrap();
        write_prec(&n1.join("101.aa.prec"), &[(5, Source::Binance, &t3)], false);

        let u = load_union(&[dir.join("n1"), dir.join("n2")]).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(u.node_frames, vec![3, 2]);
        assert_eq!(u.frames.len(), 5);
        assert!(u.frames.windows(2).all(|w| w[0].rx_us <= w[1].rx_us));
        assert_eq!(u.torn_files, 1);
        let b = ha::key::Feed::Binance.index();
        // The trade key is the aggregate id and its time, not the price,
        // so key 2 matches and key 3 is one key with two contents.
        assert_eq!(u.content.compared[b], 2);
        assert_eq!(u.content.mismatch[b], 1);
    }
}
