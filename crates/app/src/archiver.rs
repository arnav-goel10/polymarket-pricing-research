//! The archiver: holds the archive lease and, while it leads, ships each
//! closed local slot to the store as one gzipped recording.
//!
//! It runs on its own thread with blocking store calls. Every 50 ms it
//! drives the lease ([`ha::caslease`]); every 5 s, while it leads, it takes
//! each closed local slot above the committed head (looking back at most
//! `lookback` slots), streams that slot's local segment files through a
//! `RecordWriter` into a gzip temp file (constant memory), uploads it as
//! `seg/e<epoch>/s<slot>.prec.gz` with a create-only put, and commits the
//! slot in HEAD. A new leader starts at the previous `to + 1` from its own
//! segments, so a failover leaves no hole while the outage is shorter than
//! the local retention.
//!
//! A slot is closed once it is at least two slots old and below the slot
//! the node's own writer has open: the writer only ever adds to its open
//! file, so every slot below it is final on disk. With no slot open (just
//! booted, or pausing after a disk error) nothing is closed. A closed slot
//! with no local file is committed without an object and counted as a
//! hole; with no committed head yet the archiver waits for local data
//! instead. A shipped slot whose files may miss frames (a writer killed or
//! a file abandoned in it, or no writer already running when it began) is
//! counted as partial and logged.
//!
//! A new leader's first commit is preceded by an overlap object: its own
//! copy of the previous tenure's last slot, under its own epoch. Each node
//! files a frame by its own receive time, so near a slot boundary the old
//! leader can file an event one slot later than the new leader; the old
//! leader's copy is then in no committed object and the new leader's copy
//! is in the overlap. Readers merge an overlap with its slot by key.
//!
//! Every lease event is appended to `lease.jsonl` in the data directory.
//! On stop the archiver releases the lease (holder null, epoch kept) and
//! exits within two seconds.

use feeds::recording::{RecordReader, RecordWriter};
use flate2::write::GzEncoder;
use flate2::Compression;
use ha::caslease::{self, CasLease, LeaseEvent};
use ha::store::{HeadStore, ObjectStore, PutOutcome};
use serde::Serialize;
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

const TICK: Duration = Duration::from_millis(50);
const ARCHIVE_EVERY_MS: u64 = 5_000;
const RELEASE_WAIT_MS: u64 = 2_000;

/// What the rest of the node sees of the lease and the archive.
#[derive(Debug)]
pub struct LeaseView {
    /// The newest epoch in HEAD as this node last saw it.
    pub epoch: AtomicU64,
    /// Milliseconds after `start` until which this node believes it leads;
    /// 0 when it does not. Read through [`LeaseView::leader`].
    pub leader_until: AtomicU64,
    /// The monotonic origin the lease runs on.
    pub start: Instant,
    pub fence_rejected: AtomicU64,
    pub commits: AtomicU64,
    pub uploads: AtomicU64,
    pub upload_errors: AtomicU64,
    pub holes: AtomicU64,
    /// Shipped slots whose local files may miss frames.
    pub partials: AtomicU64,
    pub overlaps: AtomicU64,
    pub unknown_outcomes: AtomicU64,
    /// The slot the node's segment writer has open, or `i64::MIN`.
    pub open_slot: AtomicI64,
}

impl LeaseView {
    /// Whether this node believes it leads right now. Judged at the moment
    /// of asking, so a node frozen past its deadline stops saying it leads
    /// as soon as it wakes, before the archiver thread has run again.
    pub fn leader(&self) -> bool {
        (self.start.elapsed().as_millis() as u64) < self.leader_until.load(Relaxed)
    }
}

impl Default for LeaseView {
    fn default() -> Self {
        LeaseView {
            epoch: AtomicU64::new(0),
            leader_until: AtomicU64::new(0),
            start: Instant::now(),
            fence_rejected: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            uploads: AtomicU64::new(0),
            upload_errors: AtomicU64::new(0),
            holes: AtomicU64::new(0),
            partials: AtomicU64::new(0),
            overlaps: AtomicU64::new(0),
            unknown_outcomes: AtomicU64::new(0),
            open_slot: AtomicI64::new(i64::MIN),
        }
    }
}

/// The archiver over any store: the directory store in CI, Blob Storage on
/// a VM.
pub struct Archiver<S> {
    pub lease: CasLease,
    pub store: S,
    pub seg_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub log: PathBuf,
    pub node: u32,
    pub slot_micros: i64,
    pub lookback: i64,
    pub view: Arc<LeaseView>,
    pub stop: Arc<AtomicBool>,
}

#[derive(Serialize)]
struct LogLine {
    wall_ms: i64,
    node: u32,
    ev: &'static str,
    epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    slot: Option<i64>,
}

fn wall_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// The slot number of a segment file name, `<slot>.<boot>.prec[.open]`.
pub fn file_slot(name: &str) -> Option<i64> {
    if !(name.ends_with(".prec") || name.ends_with(".prec.open")) {
        return None;
    }
    name.split('.').next()?.parse().ok()
}

/// Every local file of `slot`, in name order.
fn slot_files(dir: &Path, slot: i64) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| file_slot(&e.file_name().to_string_lossy()) == Some(slot))
        .map(|e| e.path())
        .collect();
    out.sort();
    out
}

/// Whether a slot's local files may miss frames: a writer was killed in it
/// (a dead boot's `.open` file; the live writer's open slot is never
/// shipped), a writer abandoned a file in it (a part file), or no boot with
/// a file in it has a file in an earlier slot, so no writer was already
/// running when it began.
fn may_be_partial(dir: &Path, slot: i64, files: &[PathBuf]) -> bool {
    let names: Vec<String> = files
        .iter()
        .filter_map(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .collect();
    let cut = |n: &String| {
        n.ends_with(".open") || n.split('.').nth(2).is_some_and(|p| p.starts_with('r'))
    };
    if names.iter().any(cut) {
        return true;
    }
    let boots: HashSet<String> = names
        .iter()
        .filter_map(|n| n.split('.').nth(1))
        .map(str::to_string)
        .collect();
    let earlier = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            file_slot(&n).is_some_and(|s| s < slot)
                && n.split('.').nth(1).is_some_and(|b| boots.contains(b))
        });
    !earlier
}

/// The oldest slot with a local file.
fn oldest_slot(dir: &Path) -> Option<i64> {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| file_slot(&e.file_name().to_string_lossy()))
        .min()
}

/// Stream `files` into one gzipped recording at `out`. A file's torn tail
/// (a killed writer) ends that file. Returns the records written.
pub fn gzip_slot(files: &[PathBuf], out: &Path) -> io::Result<u64> {
    let gz = GzEncoder::new(BufWriter::new(File::create(out)?), Compression::fast());
    let mut w = RecordWriter::new(gz)?;
    for p in files {
        let Ok(r) = RecordReader::open(p) else {
            continue;
        };
        for rec in r {
            match rec {
                Ok(rec) => w.append(rec.recv_micros, rec.source, &rec.frame)?,
                Err(_) => break,
            }
        }
    }
    let n = w.records();
    let mut bw = w.into_inner()?.finish()?;
    bw.flush()?;
    bw.into_inner().map_err(|e| e.into_error())?.sync_all()?;
    Ok(n)
}

impl<S: HeadStore + ObjectStore> Archiver<S> {
    fn now(start: Instant) -> u64 {
        start.elapsed().as_millis() as u64
    }

    fn drive(&mut self, start: Instant) {
        caslease::drive(&mut self.lease, &self.store, &mut || Self::now(start));
        let now = Self::now(start);
        let v = &self.view;
        v.leader_until
            .store(self.lease.lead_until(now).unwrap_or(0), Relaxed);
        if let Some(h) = self.lease.last_head() {
            v.epoch.store(h.epoch, Relaxed);
        }
        let st = self.lease.stats();
        v.fence_rejected.store(st.fence_rejected, Relaxed);
        v.commits.store(st.commits, Relaxed);
        v.unknown_outcomes.store(st.unknown_outcomes, Relaxed);
        for e in self.lease.take_events() {
            self.log(e);
        }
    }

    fn log(&self, e: LeaseEvent) {
        let (ev, epoch, slot) = match e {
            LeaseEvent::Granted { epoch } => ("grant", epoch, None),
            LeaseEvent::Committed { epoch, slot } => ("commit", epoch, Some(slot)),
            LeaseEvent::Fenced { epoch } => ("fenced", epoch, None),
            LeaseEvent::Released { epoch } => ("release", epoch, None),
        };
        self.log_line(ev, epoch, slot);
    }

    fn log_line(&self, ev: &'static str, epoch: u64, slot: Option<i64>) {
        let line = LogLine {
            wall_ms: wall_micros() / 1_000,
            node: self.node,
            ev,
            epoch,
            slot,
        };
        let Ok(mut s) = serde_json::to_string(&line) else {
            return;
        };
        s.push('\n');
        let r = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .and_then(|mut f| f.write_all(s.as_bytes()));
        if let Err(err) = r {
            tracing::warn!(error = %err, "lease log write failed");
        }
    }

    /// Ship every closed slot above the committed head while this node
    /// leads.
    fn archive_pass(&mut self, start: Instant) {
        let now_slot = wall_micros().div_euclid(self.slot_micros);
        let open = self.view.open_slot.load(Relaxed);
        if open == i64::MIN {
            // Just booted, or pausing after a disk error: frames may still
            // be queued for any slot.
            return;
        }
        let newest = (now_slot - 2).min(open - 1);
        let floor = now_slot - 2 - self.lookback + 1;
        loop {
            if !self.lease.can_commit(Self::now(start)) {
                return;
            }
            let Some(epoch) = self.lease.held_epoch() else {
                return;
            };
            let committed = self.lease.committed_to();
            let next = match committed {
                Some(c) => c + 1,
                None => match oldest_slot(&self.seg_dir) {
                    Some(s) => s,
                    None => return,
                },
            };
            let next = next.max(floor);
            if next > newest {
                return;
            }
            // Before this epoch's first commit, its copy of the slot the
            // previous tenure ended on.
            let first = self
                .lease
                .last_head()
                .and_then(|h| h.tenures.last())
                .is_none_or(|t| t.e != epoch);
            if let (true, Some(c)) = (first, committed) {
                let files = slot_files(&self.seg_dir, c);
                if !files.is_empty() {
                    if !self.upload(epoch, c, &files) {
                        return;
                    }
                    self.view.overlaps.fetch_add(1, Relaxed);
                    self.log_line("overlap", epoch, Some(c));
                }
            }
            let skipped = next - committed.map_or(next, |c| c + 1);
            let files = slot_files(&self.seg_dir, next);
            if files.is_empty() {
                if committed.is_none() {
                    return;
                }
                self.view.holes.fetch_add(1 + skipped as u64, Relaxed);
                self.log_line("hole", epoch, Some(next));
            } else {
                if !self.upload(epoch, next, &files) {
                    return;
                }
                self.view.holes.fetch_add(skipped as u64, Relaxed);
                if may_be_partial(&self.seg_dir, next, &files) {
                    self.view.partials.fetch_add(1, Relaxed);
                    self.log_line("partial", epoch, Some(next));
                }
            }
            self.lease.commit(next);
            self.drive(start);
            if self.lease.commit_pending() {
                // Unknown or unreachable: the next pass tries again.
                return;
            }
        }
    }

    /// Upload one slot. True when the object exists in the store.
    fn upload(&mut self, epoch: u64, slot: i64, files: &[PathBuf]) -> bool {
        let tmp = self.tmp_dir.join(format!("{slot}.prec.gz.tmp"));
        let ok = match gzip_slot(files, &tmp) {
            Ok(_) => match self
                .store
                .put_new(&caslease::object_name(epoch, slot), &tmp)
            {
                // An existing object under my epoch is my earlier attempt.
                PutOutcome::Created | PutOutcome::PreconditionFailed => true,
                PutOutcome::Unknown | PutOutcome::Unreachable => false,
            },
            Err(e) => {
                tracing::warn!(error = %e, slot, "gzip of a slot failed");
                false
            }
        };
        let _ = std::fs::remove_file(&tmp);
        if ok {
            self.view.uploads.fetch_add(1, Relaxed);
        } else {
            self.view.upload_errors.fetch_add(1, Relaxed);
        }
        ok
    }

    /// Run until `stop` is set, then release and return.
    pub fn run(mut self) {
        if let Err(e) = std::fs::create_dir_all(&self.tmp_dir) {
            tracing::warn!(error = %e, "archive temp dir");
        }
        let start = self.view.start;
        let mut last_pass: Option<u64> = None;
        let mut release_by: Option<u64> = None;
        loop {
            let now = Self::now(start);
            if self.stop.load(Relaxed) && release_by.is_none() {
                self.lease.release();
                release_by = Some(now + RELEASE_WAIT_MS);
            }
            self.drive(start);
            if let Some(by) = release_by {
                if self.lease.is_stopped() || Self::now(start) >= by {
                    self.view.leader_until.store(0, Relaxed);
                    return;
                }
            } else if last_pass.is_none_or(|t| now >= t + ARCHIVE_EVERY_MS) {
                last_pass = Some(now);
                self.archive_pass(start);
            }
            std::thread::sleep(TICK);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feeds::recording::Source;
    use ha::caslease::{Config, Head};
    use ha::store::FsStore;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("archiver-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn write_seg(path: &Path, n: usize, torn: bool) {
        let mut w = RecordWriter::create(path).unwrap();
        for i in 0..n {
            w.append(i as i64, Source::Rtds, &format!("{{\"i\":{i}}}"))
                .unwrap();
        }
        w.finish().unwrap();
        if torn {
            let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
            f.write_all(&[9, 0, 0, 0, 1]).unwrap();
        }
    }

    #[test]
    fn a_slot_is_every_local_file_of_that_minute_gzipped_with_torn_tails_cut() {
        let d = scratch("gzip");
        write_seg(&d.join("100.aaaa.prec"), 3, false);
        write_seg(&d.join("100.bbbb.prec.open"), 2, true);
        write_seg(&d.join("101.aaaa.prec"), 5, false);
        std::fs::write(d.join("notes.txt"), b"x").unwrap();
        let files = slot_files(&d, 100);
        assert_eq!(files.len(), 2);
        assert_eq!(oldest_slot(&d), Some(100));
        let out = d.join("100.gz");
        assert_eq!(gzip_slot(&files, &out).unwrap(), 5);
        let mut raw = Vec::new();
        std::io::Read::read_to_end(
            &mut flate2::read::GzDecoder::new(File::open(&out).unwrap()),
            &mut raw,
        )
        .unwrap();
        let (recs, err) = feeds::recording::read_bytes(&raw);
        assert!(err.is_none());
        assert_eq!(recs.len(), 5);
        let _ = std::fs::remove_dir_all(d);
    }

    fn archiver(d: &Path, node: u32, slot_micros: i64, view: Arc<LeaseView>) -> Archiver<FsStore> {
        Archiver {
            lease: CasLease::new(Config::new(node, node as u64)),
            store: FsStore::open(d.join("store")).unwrap(),
            seg_dir: d.join("seg"),
            tmp_dir: d.join("tmp"),
            log: d.join("lease.jsonl"),
            node,
            slot_micros,
            lookback: 60,
            view,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// An hour, so the wall-clock slot does not roll during a test.
    const HOUR: i64 = 3_600_000_000;

    #[test]
    fn nothing_at_or_above_the_writers_open_slot_is_committed() {
        let d = scratch("open");
        let seg = d.join("seg");
        std::fs::create_dir_all(&seg).unwrap();
        let now_slot = wall_micros().div_euclid(HOUR);
        write_seg(&seg.join(format!("{}.aaaa.prec", now_slot - 6)), 3, false);
        write_seg(&seg.join(format!("{}.aaaa.prec", now_slot - 5)), 3, false);
        write_seg(&seg.join(format!("{}.bbbb.prec", now_slot - 5)), 3, false);
        // A boot that started and was killed inside now-4.
        write_seg(
            &seg.join(format!("{}.cccc.prec.open", now_slot - 4)),
            3,
            true,
        );
        let view = Arc::new(LeaseView::default());
        let mut a = archiver(&d, 1, HOUR, view.clone());
        std::fs::create_dir_all(&a.tmp_dir).unwrap();
        let start = view.start;
        a.drive(start);
        assert_eq!(a.lease.held_epoch(), Some(1));
        // Just booted: no slot open, so frames may be queued for any slot.
        a.archive_pass(start);
        assert_eq!(a.lease.committed_to(), None);
        // The writer is behind, with now-4 open: now-3 and now-2 are not
        // written yet and must not be committed as holes.
        view.open_slot.store(now_slot - 4, Relaxed);
        a.archive_pass(start);
        assert_eq!(a.lease.committed_to(), Some(now_slot - 5));
        assert_eq!(view.holes.load(Relaxed), 0);
        // now-6 began before any writer here was running.
        assert_eq!(view.partials.load(Relaxed), 1);
        // The writer moves on: now-3 and now-2 are final and empty.
        view.open_slot.store(now_slot, Relaxed);
        a.archive_pass(start);
        assert_eq!(a.lease.committed_to(), Some(now_slot - 2));
        assert_eq!(view.holes.load(Relaxed), 2);
        // now-4 holds a killed writer's file.
        assert_eq!(view.partials.load(Relaxed), 2);
        assert_eq!(view.overlaps.load(Relaxed), 0);
        let log = std::fs::read_to_string(d.join("lease.jsonl")).unwrap();
        assert_eq!(log.matches("\"partial\"").count(), 2);
        assert_eq!(log.matches("\"hole\"").count(), 2);
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn a_new_leader_ships_its_copy_of_the_previous_last_slot_as_an_overlap() {
        let d = scratch("overlap");
        let seg = d.join("seg");
        std::fs::create_dir_all(&seg).unwrap();
        let now_slot = wall_micros().div_euclid(HOUR);
        for s in now_slot - 7..=now_slot - 2 {
            write_seg(&seg.join(format!("{s}.bbbb.prec")), 2, false);
        }
        // Node 1 led epoch 1 up to now-6 and released.
        let store = FsStore::open(d.join("store")).unwrap();
        let prev = Head {
            v: 1,
            epoch: 1,
            holder: None,
            ttl_ms: 30_000,
            nonce: "n".into(),
            tenures: vec![caslease::Tenure {
                e: 1,
                n: 1,
                from: now_slot - 8,
                to: now_slot - 6,
            }],
        };
        assert!(matches!(
            store.write_head(&prev.encode(), &ha::store::Cond::IfNoneMatch),
            ha::store::WriteOutcome::Done(_)
        ));
        let view = Arc::new(LeaseView::default());
        view.open_slot.store(now_slot, Relaxed);
        let mut a = archiver(&d, 2, HOUR, view.clone());
        std::fs::create_dir_all(&a.tmp_dir).unwrap();
        let start = view.start;
        a.drive(start);
        assert_eq!(a.lease.held_epoch(), Some(2));
        a.archive_pass(start);
        assert_eq!(a.lease.committed_to(), Some(now_slot - 2));
        let (body, _) = store.read_head().unwrap().unwrap();
        let head = Head::decode(&body).unwrap();
        assert_eq!(head.tenures[1].from, now_slot - 5);
        let names = store.list("seg/").unwrap();
        assert!(names.contains(&caslease::object_name(2, now_slot - 6)));
        let v = caslease::read_archive(&head, &names);
        // Slots now-8 to now-6 have no epoch-1 objects in this store.
        assert_eq!((v.archived, v.holes, v.overlaps, v.orphans), (4, 3, 1, 0));
        assert_eq!(view.overlaps.load(Relaxed), 1);
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn the_leader_ships_closed_slots_in_order_and_releases_on_stop() {
        let d = scratch("ship");
        let seg = d.join("seg");
        std::fs::create_dir_all(&seg).unwrap();
        let slot_micros = 60_000_000;
        let now_slot = wall_micros().div_euclid(slot_micros);
        for s in now_slot - 5..=now_slot {
            write_seg(&seg.join(format!("{s}.aaaa.prec")), 4, false);
        }
        let store = FsStore::open(d.join("store")).unwrap();
        let view = Arc::new(LeaseView::default());
        view.open_slot.store(now_slot, Relaxed);
        let stop = Arc::new(AtomicBool::new(false));
        let a = Archiver {
            lease: CasLease::new(Config::new(1, 1)),
            store: store.clone(),
            seg_dir: seg,
            tmp_dir: d.join("tmp"),
            log: d.join("lease.jsonl"),
            node: 1,
            slot_micros,
            lookback: 60,
            view: view.clone(),
            stop: stop.clone(),
        };
        let t = std::thread::spawn(move || a.run());
        let started = Instant::now();
        while view.commits.load(Relaxed) < 4 && started.elapsed() < Duration::from_secs(10) {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(view.leader());
        stop.store(true, Relaxed);
        t.join().unwrap();
        assert!(!view.leader());
        let (body, _) = store.read_head().unwrap().unwrap();
        let head = Head::decode(&body).unwrap();
        assert_eq!(head.holder, None, "released on stop");
        // Slots now-5 to now-2 are closed; now-1 is too recent unless the
        // minute rolled over during the test.
        let to = head.committed_to().unwrap();
        assert!(
            to == now_slot - 2 || to == now_slot - 1,
            "committed to {to}"
        );
        assert_eq!(head.tenures[0].from, now_slot - 5);
        let names = store.list("seg/").unwrap();
        let v = caslease::read_archive(&head, &names);
        let want = (to - (now_slot - 5) + 1) as u64;
        assert_eq!((v.archived, v.holes, v.orphans), (want, 0, 0));
        let au = caslease::audit_bodies(&FsStore::read_journal(store.root()).unwrap());
        assert_eq!(au.reason(), None);
        let log = std::fs::read_to_string(d.join("lease.jsonl")).unwrap();
        assert_eq!(
            log.lines().filter(|l| l.contains("\"commit\"")).count() as u64,
            want
        );
        assert!(log.contains("\"release\""));
        let _ = std::fs::remove_dir_all(d);
    }
}
