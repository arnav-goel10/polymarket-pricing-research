//! The node's local minute segments.
//!
//! Files are `<dir>/<slot>.<boot>.prec.open` while written and `.prec` once
//! closed, where the slot is `rx_micros` divided by the slot length. A slot
//! that gets a second file from the same boot (after a write error, or a
//! clock that stepped back more than a slot) names it
//! `<slot>.<boot>.r<part>.prec`, so a file is never reopened or truncated
//! and name order is write order within a boot.
//!
//! Rules, each tested below:
//!
//! - A frame of a later slot closes the open file (renamed to `.prec`) and
//!   opens that slot. Files are cut by arrival order, so a frame stamped
//!   near the end of a slot can land in the next slot's file.
//! - A frame up to one slot behind the open slot is written to the open
//!   file. A frame further behind closes the open file and opens its own
//!   slot, so a clock that jumped forward and came back does not keep the
//!   writer in the future.
//! - The open file is flushed when a frame arrives at least
//!   [`FLUSH_EVERY`] after the last flush, not on a timer.
//! - Opening a slot deletes every file, from any boot, whose slot is
//!   `keep` or more behind it. The slot used for that is capped at the
//!   slot this writer first opened plus the monotonic time since, so a wall
//!   clock that jumps forward cannot delete the retained files.
//! - Any append or flush error abandons the open file: it is closed as is
//!   (its tail may be torn, which ends that file for a reader) and nothing
//!   is written for [`FLUSH_EVERY`]. The next frame after that opens a new
//!   file. One failure costs the failing frame, whatever was still
//!   buffered, and the frames of that second, never the rest of the slot.

use crate::archiver::LeaseView;
use feeds::recording::{RecordWriter, Source};
use ha::wire::Boot;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const FLUSH_EVERY: Duration = Duration::from_secs(1);

/// A segment file: something to write records to and to sync at close.
pub trait SegFile: Write + Send {
    fn sync(&mut self) -> io::Result<()>;
}

impl SegFile for File {
    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

/// Creates a new segment file at a path that must not exist yet.
pub type Opener = Box<dyn FnMut(&Path) -> io::Result<Box<dyn SegFile>> + Send>;

/// The real opener: a new file, never an existing one.
pub fn create_new() -> Opener {
    Box::new(|p: &Path| {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(p)?;
        Ok(Box::new(f) as Box<dyn SegFile>)
    })
}

#[derive(Debug, Default)]
pub struct SegStats {
    /// Append, flush, close, rename or prune failures.
    pub errors: AtomicU64,
    /// Frames not written: the failing frame, and every frame during the
    /// pause after a failure.
    pub lost: AtomicU64,
    pub closed: AtomicU64,
}

/// A seeded fault, used only by the tests to show the checks can fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
enum SegMutant {
    None,
    /// Keep appending to a writer after it failed.
    KeepWriter,
    /// Prune by the newest slot seen, however far the clock jumped.
    PruneByWallClock,
}

struct Open {
    slot: i64,
    path: PathBuf,
    w: RecordWriter<Box<dyn SegFile>>,
    last_flush: Instant,
}

pub struct Segments {
    dir: PathBuf,
    boot: Boot,
    keep: i64,
    slot_micros: i64,
    view: Arc<LeaseView>,
    pub stats: Arc<SegStats>,
    opener: Opener,
    open: Option<Open>,
    /// The highest slot that has had a file from this writer.
    max_opened: i64,
    part: u32,
    retry_at: Option<Instant>,
    /// The first slot opened and when, for the prune cap.
    origin: Option<(i64, Instant)>,
    /// Records and bytes in files this writer has closed or abandoned.
    pub records: u64,
    pub bytes: u64,
    mutant: SegMutant,
}

impl Segments {
    pub fn new(
        dir: PathBuf,
        boot: Boot,
        keep: i64,
        slot_micros: i64,
        view: Arc<LeaseView>,
        opener: Opener,
    ) -> Self {
        Segments {
            dir,
            boot,
            keep,
            slot_micros,
            view,
            stats: Arc::default(),
            opener,
            open: None,
            max_opened: i64::MIN,
            part: 0,
            retry_at: None,
            origin: None,
            records: 0,
            bytes: 0,
            mutant: SegMutant::None,
        }
    }

    #[cfg(test)]
    fn with_mutant(mut self, m: SegMutant) -> Self {
        self.mutant = m;
        self
    }

    /// The slot the writer has open, if any.
    pub fn open_slot(&self) -> Option<i64> {
        self.open.as_ref().map(|o| o.slot)
    }

    fn name(&self, slot: i64, part: u32, open: bool) -> PathBuf {
        let ext = if open { "prec.open" } else { "prec" };
        let name = if part == 0 {
            format!("{slot}.{}.{ext}", self.boot)
        } else {
            format!("{slot}.{}.r{part:06}.{ext}", self.boot)
        };
        self.dir.join(name)
    }

    /// Write one frame received at `now` (monotonic) and `rx_micros` (wall).
    /// Errors are counted in [`SegStats`]; the caller only logs them.
    pub fn append(
        &mut self,
        now: Instant,
        rx_micros: i64,
        src: Source,
        frame: &str,
    ) -> io::Result<()> {
        if let Some(t) = self.retry_at {
            if now < t {
                self.stats.lost.fetch_add(1, Relaxed);
                return Ok(());
            }
            self.retry_at = None;
        }
        let slot = rx_micros.div_euclid(self.slot_micros);
        let roll = self
            .open
            .as_ref()
            .is_none_or(|o| slot > o.slot || slot < o.slot - 1);
        if roll {
            let _ = self.close();
            if let Err(e) = self.open_new(now, slot) {
                self.stats.errors.fetch_add(1, Relaxed);
                self.stats.lost.fetch_add(1, Relaxed);
                self.retry_at = Some(now + FLUSH_EVERY);
                return Err(e);
            }
        }
        let o = self.open.as_mut().expect("segment open");
        let mut r = o.w.append(rx_micros, src, frame);
        if r.is_ok() && now.saturating_duration_since(o.last_flush) >= FLUSH_EVERY {
            r = o.w.flush();
            o.last_flush = now;
        }
        if let Err(e) = r {
            self.stats.errors.fetch_add(1, Relaxed);
            self.stats.lost.fetch_add(1, Relaxed);
            if self.mutant != SegMutant::KeepWriter {
                self.abandon(now);
            }
            return Err(e);
        }
        Ok(())
    }

    fn open_new(&mut self, now: Instant, slot: i64) -> io::Result<()> {
        let part = if slot > self.max_opened {
            0
        } else {
            self.part += 1;
            self.part
        };
        let path = self.name(slot, part, true);
        let w = RecordWriter::new((self.opener)(&path)?)?;
        self.open = Some(Open {
            slot,
            path,
            w,
            last_flush: now,
        });
        self.max_opened = self.max_opened.max(slot);
        self.view.open_slot.store(slot, Relaxed);
        let (o_slot, o_at) = *self.origin.get_or_insert((slot, now));
        let elapsed = now.saturating_duration_since(o_at).as_micros() as i64;
        let plausible = o_slot + elapsed / self.slot_micros + 1;
        let current = if self.mutant == SegMutant::PruneByWallClock {
            slot
        } else {
            slot.min(plausible)
        };
        self.prune(current);
        Ok(())
    }

    fn closed_path(open: &Path) -> PathBuf {
        let s = open.to_string_lossy();
        PathBuf::from(s.strip_suffix(".open").unwrap_or(&s).to_string())
    }

    /// Close the open file: flush, sync and rename it to `.prec`.
    pub fn close(&mut self) -> io::Result<()> {
        let Some(o) = self.open.take() else {
            return Ok(());
        };
        self.view.open_slot.store(i64::MIN, Relaxed);
        self.records += o.w.records();
        self.bytes += o.w.bytes();
        let synced = o.w.into_inner().and_then(|mut f| f.sync());
        let renamed = std::fs::rename(&o.path, Self::closed_path(&o.path));
        let r = synced.and(renamed);
        if r.is_err() {
            self.stats.errors.fetch_add(1, Relaxed);
        } else {
            self.stats.closed.fetch_add(1, Relaxed);
        }
        r
    }

    /// Give up on the open file after an error. Dropping the writer makes
    /// one last attempt to write what it buffered; whatever is on disk is
    /// closed as is.
    fn abandon(&mut self, now: Instant) {
        let Some(o) = self.open.take() else {
            return;
        };
        self.view.open_slot.store(i64::MIN, Relaxed);
        self.records += o.w.records();
        self.bytes += o.w.bytes();
        drop(o.w);
        if std::fs::rename(&o.path, Self::closed_path(&o.path)).is_err() {
            self.stats.errors.fetch_add(1, Relaxed);
        } else {
            self.stats.closed.fetch_add(1, Relaxed);
        }
        self.retry_at = Some(now + FLUSH_EVERY);
    }

    /// Delete every segment, from any boot, `keep` or more slots behind
    /// `current`.
    fn prune(&self, current: i64) {
        let Ok(dir) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for e in dir.flatten() {
            let name = e.file_name();
            let Some(slot) = name
                .to_str()
                .and_then(|n| n.split('.').next())
                .and_then(|s| s.parse::<i64>().ok())
            else {
                continue;
            };
            if slot <= current - self.keep && std::fs::remove_file(e.path()).is_err() {
                self.stats.errors.fetch_add(1, Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feeds::recording::{read_all, read_bytes};
    use std::sync::atomic::AtomicBool;

    const SLOT: i64 = 60_000_000;
    const BOOT: Boot = Boot(0xab);

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("segments-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn segs(dir: &Path, opener: Opener) -> Segments {
        Segments::new(
            dir.to_path_buf(),
            BOOT,
            60,
            SLOT,
            Arc::new(LeaseView::default()),
            opener,
        )
    }

    /// File names in `dir`, sorted.
    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        v.sort();
        v
    }

    /// The `rx` of every record a reader gets, file by file in name order,
    /// each file up to its first bad record.
    fn readable(dir: &Path) -> Vec<i64> {
        let mut out = Vec::new();
        for n in names(dir) {
            let (recs, _) = read_bytes(&std::fs::read(dir.join(n)).unwrap());
            out.extend(recs.iter().map(|r| r.recv_micros));
        }
        out
    }

    fn at(slot: i64, k: i64) -> i64 {
        slot * SLOT + k
    }

    #[test]
    fn a_later_slot_closes_the_open_file_and_a_small_step_back_stays_in_it() {
        let dir = scratch("roll");
        let mut s = segs(&dir, create_new());
        let t = Instant::now();
        s.append(t, at(100, 1), Source::Rtds, "a").unwrap();
        assert_eq!(names(&dir), vec!["100.00000000000000ab.prec.open"]);
        s.append(t, at(101, 1), Source::Rtds, "b").unwrap();
        // One slot back is the clock stepping or a frame stamped before the
        // roll: it goes to the open file.
        s.append(t, at(100, 2), Source::Rtds, "c").unwrap();
        assert_eq!(s.open_slot(), Some(101));
        assert_eq!(
            names(&dir),
            vec![
                "100.00000000000000ab.prec",
                "101.00000000000000ab.prec.open"
            ]
        );
        s.close().unwrap();
        assert_eq!(
            names(&dir),
            vec!["100.00000000000000ab.prec", "101.00000000000000ab.prec"]
        );
        assert_eq!(readable(&dir), vec![at(100, 1), at(101, 1), at(100, 2)]);
        assert_eq!((s.records, s.stats.closed.load(Relaxed)), (3, 2));
    }

    #[test]
    fn flushes_only_when_a_frame_comes_a_second_after_the_last_flush() {
        let dir = scratch("flush");
        let mut s = segs(&dir, create_new());
        let t = Instant::now();
        let open = dir.join("5.00000000000000ab.prec.open");
        s.append(t, at(5, 1), Source::Rtds, "a").unwrap();
        s.append(t + Duration::from_millis(900), at(5, 2), Source::Rtds, "b")
            .unwrap();
        assert_eq!(std::fs::metadata(&open).unwrap().len(), 0, "flushed early");
        s.append(t + Duration::from_millis(1000), at(5, 3), Source::Rtds, "c")
            .unwrap();
        let (recs, tail) = read_all(&open).unwrap();
        assert_eq!((recs.len(), tail), (3, None));
        // The next flush is a second after that one, not after the first.
        s.append(t + Duration::from_millis(1500), at(5, 4), Source::Rtds, "d")
            .unwrap();
        assert_eq!(read_all(&open).unwrap().0.len(), 3);
    }

    #[test]
    fn prunes_any_boot_sixty_slots_back_and_keeps_fifty_nine() {
        let dir = scratch("prune");
        for slot in [40, 50, 59, 60, 61] {
            std::fs::write(dir.join(format!("{slot}.00000000000000cd.prec")), b"").unwrap();
        }
        std::fs::write(dir.join("notes.txt"), b"").unwrap();
        let mut s = segs(&dir, create_new());
        s.append(Instant::now(), at(120, 0), Source::Rtds, "a")
            .unwrap();
        assert_eq!(
            names(&dir),
            vec![
                "120.00000000000000ab.prec.open",
                "61.00000000000000cd.prec",
                "notes.txt"
            ]
        );
    }

    /// The clock jumps a day ahead for one frame and comes back.
    fn forward_jump(mutant: SegMutant) -> (Vec<String>, Option<i64>) {
        let dir = scratch(&format!("jump-{mutant:?}"));
        for slot in 50..100 {
            std::fs::write(dir.join(format!("{slot}.00000000000000cd.prec")), b"").unwrap();
        }
        let mut s = segs(&dir, create_new()).with_mutant(mutant);
        let t = Instant::now();
        s.append(t, at(100, 0), Source::Rtds, "a").unwrap();
        s.append(t, at(100 + 1440, 0), Source::Rtds, "b").unwrap();
        s.append(t, at(100, 1), Source::Rtds, "c").unwrap();
        (names(&dir), s.open_slot())
    }

    #[test]
    fn a_clock_that_jumps_forward_and_back_keeps_retention_and_the_slot() {
        let (names, open) = forward_jump(SegMutant::None);
        // Nothing retained was deleted, and the writer is back in the
        // present instead of pinned a day ahead.
        assert_eq!(names.iter().filter(|n| n.ends_with("cd.prec")).count(), 50);
        assert_eq!(open, Some(100));
        assert!(
            names.contains(&"100.00000000000000ab.r000001.prec.open".to_string()),
            "{names:?}"
        );
        assert!(names.contains(&"100.00000000000000ab.prec".to_string()));
        assert!(names.contains(&"1540.00000000000000ab.prec".to_string()));
        // Control: pruning by the wall clock alone deletes all of it.
        let (names, _) = forward_jump(SegMutant::PruneByWallClock);
        assert_eq!(names.iter().filter(|n| n.ends_with("cd.prec")).count(), 0);
    }

    #[test]
    fn prune_follows_monotonic_time_once_it_has_passed() {
        let dir = scratch("prune-later");
        std::fs::write(dir.join("10.00000000000000cd.prec"), b"").unwrap();
        let mut s = segs(&dir, create_new());
        let t = Instant::now();
        s.append(t, at(69, 0), Source::Rtds, "a").unwrap();
        assert_eq!(names(&dir).len(), 2);
        // A real minute later the slot 60 behind goes.
        s.append(t + Duration::from_secs(60), at(70, 0), Source::Rtds, "b")
            .unwrap();
        assert!(!names(&dir).contains(&"10.00000000000000cd.prec".to_string()));
    }

    /// A file whose first write fails once, as a full disk would.
    struct FailOnce {
        f: File,
        armed: Arc<AtomicBool>,
    }

    impl Write for FailOnce {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.armed.swap(false, Relaxed) {
                return Err(io::Error::other("no space left"));
            }
            self.f.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.f.flush()
        }
    }

    impl SegFile for FailOnce {
        fn sync(&mut self) -> io::Result<()> {
            self.f.sync_all()
        }
    }

    /// Append 40 KiB frames, one per 100 ms, through a disk that fails one
    /// write. Returns the frames reported written and the frames readable.
    fn one_failure(mutant: SegMutant) -> (Vec<i64>, Vec<i64>, u64) {
        let dir = scratch(&format!("fail-{mutant:?}"));
        let armed = Arc::new(AtomicBool::new(true));
        let a = armed.clone();
        let opener: Opener = Box::new(move |p: &Path| {
            let f = File::create_new(p)?;
            Ok(Box::new(FailOnce {
                f,
                armed: a.clone(),
            }) as Box<dyn SegFile>)
        });
        let mut s = segs(&dir, opener).with_mutant(mutant);
        let frame = "x".repeat(40 * 1024);
        let t = Instant::now();
        let mut ok = Vec::new();
        for i in 0..40i64 {
            let now = t + Duration::from_millis(100 * i as u64);
            let lost = s.stats.lost.load(Relaxed);
            if s.append(now, at(7, i), Source::ClobBook, &frame).is_ok()
                && s.stats.lost.load(Relaxed) == lost
            {
                ok.push(at(7, i));
            }
        }
        s.close().unwrap();
        assert!(!armed.load(Relaxed), "the failure never happened");
        (ok, readable(&dir), s.stats.lost.load(Relaxed))
    }

    #[test]
    fn a_write_error_costs_a_bounded_tail_not_the_rest_of_the_slot() {
        // The second frame does not fit the 64 KiB buffer, so its flush
        // fails with its record header already buffered: a torn record.
        let (ok, readable_rx, lost) = one_failure(SegMutant::None);
        assert_eq!(readable_rx, ok, "a frame reported written is unreadable");
        // The failing frame and the frames of the pause after it.
        assert!((1..=11).contains(&lost), "lost {lost}");
        assert_eq!(ok.len() as u64 + lost, 40);
        // Control: writing on after the tear makes every later frame of
        // the slot unreadable.
        let (ok, readable_rx, _) = one_failure(SegMutant::KeepWriter);
        assert!(readable_rx.len() < ok.len() / 2, "{readable_rx:?}");
    }
}
