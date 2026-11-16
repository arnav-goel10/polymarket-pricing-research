//! Stores behind the archive lease: one HEAD document that changes only by
//! conditional writes, and write-once archive objects.
//!
//! The lease ([`crate::caslease`]) needs two operations on HEAD, a read
//! that returns the body with its ETag and a write that applies only if a
//! condition holds. The archiver needs a create-only put. Every outcome a
//! real blob store can give is named, including the one that matters most:
//! [`WriteOutcome::Unknown`], a request that timed out after it was sent
//! and may or may not have been applied.
//!
//! Two stores live here:
//!
//! - [`MemStore`]: in process, with injected faults (slow, unreachable,
//!   applied with the reply lost, lost on the way). The lease tests and the
//!   contract suite run against it.
//! - [`FsStore`]: a directory shared by several processes on one host. A
//!   lock file taken with `File::lock` makes each conditional write atomic,
//!   and every applied HEAD write is appended to a journal, so a run can be
//!   audited afterwards. `chaos local` points both nodes at one.
//!
//! [`contract_suite`] checks the semantics the lease relies on and runs
//! against every store.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// An opaque version tag. Two reads return the same ETag only if no write
/// was applied in between.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Etag(pub String);

/// The condition on a HEAD write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cond {
    /// Apply only if the current ETag is this one.
    IfMatch(Etag),
    /// Apply only if there is no HEAD yet (`If-None-Match: *`).
    IfNoneMatch,
    /// Apply whatever is there. The lease never sends this; it exists so
    /// the tests can show what a write without a fence does.
    Unconditional,
}

/// The result of a HEAD write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    /// Applied. The new ETag.
    Done(Etag),
    /// The condition did not hold and nothing was written (412, or 409 on
    /// a create).
    PreconditionFailed,
    /// The request may or may not have been applied: it timed out after it
    /// was sent.
    Unknown,
    /// The request was refused before it reached the store. Nothing was
    /// written.
    Unreachable,
}

/// The result of a create-only object put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    /// The object already exists (409 or 412). On a retry of a put whose
    /// outcome was unknown, this means the earlier attempt landed.
    PreconditionFailed,
    Unknown,
    Unreachable,
}

/// A read or list that did not get an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "store error: {}", self.0)
    }
}

impl std::error::Error for StoreError {}

pub type HeadRead = Result<Option<(Vec<u8>, Etag)>, StoreError>;

/// The HEAD document: one body, changed only by conditional writes.
pub trait HeadStore {
    fn read_head(&self) -> HeadRead;
    /// Write `body` if `cond` holds. `body` must not contain a newline.
    fn write_head(&self, body: &[u8], cond: &Cond) -> WriteOutcome;
}

/// Write-once objects under slash-separated names.
pub trait ObjectStore {
    /// Create `name` with the contents of the file at `src`, only if no
    /// object of that name exists.
    fn put_new(&self, name: &str, src: &Path) -> PutOutcome;
    /// Every object name starting with `prefix`, sorted.
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}

impl<S: HeadStore + ?Sized> HeadStore for &S {
    fn read_head(&self) -> HeadRead {
        (**self).read_head()
    }
    fn write_head(&self, body: &[u8], cond: &Cond) -> WriteOutcome {
        (**self).write_head(body, cond)
    }
}

impl<S: HeadStore + ?Sized> HeadStore for Arc<S> {
    fn read_head(&self) -> HeadRead {
        (**self).read_head()
    }
    fn write_head(&self, body: &[u8], cond: &Cond) -> WriteOutcome {
        (**self).write_head(body, cond)
    }
}

impl<S: ObjectStore + ?Sized> ObjectStore for &S {
    fn put_new(&self, name: &str, src: &Path) -> PutOutcome {
        (**self).put_new(name, src)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        (**self).list(prefix)
    }
}

impl<S: ObjectStore + ?Sized> ObjectStore for Arc<S> {
    fn put_new(&self, name: &str, src: &Path) -> PutOutcome {
        (**self).put_new(name, src)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        (**self).list(prefix)
    }
}

/// Whether a write's condition holds against the current ETag.
fn cond_holds(cond: &Cond, current: Option<&Etag>) -> bool {
    match cond {
        Cond::IfMatch(e) => current == Some(e),
        Cond::IfNoneMatch => current.is_none(),
        Cond::Unconditional => true,
    }
}

/// A fault injected into every request a [`MemStore`] serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Fault {
    #[default]
    None,
    /// Each request waits this long, then is served normally.
    Slow(Duration),
    /// Each request fails before it reaches the store.
    Unreachable,
    /// Writes are applied but the reply is lost: the caller sees
    /// `Unknown`. Reads fail.
    AppliedReplyLost,
    /// Writes are lost on the way: nothing is applied and the caller sees
    /// `Unknown`. Reads fail.
    RequestLost,
    /// A broken store that ignores every condition. Only for showing the
    /// contract suite can fail.
    IgnoreConditions,
}

#[derive(Debug, Default)]
struct MemInner {
    head: Option<(Vec<u8>, Etag)>,
    version: u64,
    objects: BTreeMap<String, Vec<u8>>,
    journal: Vec<Vec<u8>>,
    fault: Fault,
}

/// An in-process store with injected faults. Clones share the same state.
#[derive(Debug, Clone, Default)]
pub struct MemStore(Arc<Mutex<MemInner>>);

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, MemInner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set_fault(&self, f: Fault) {
        self.lock().fault = f;
    }

    /// Every applied HEAD write, in order.
    pub fn journal(&self) -> Vec<Vec<u8>> {
        self.lock().journal.clone()
    }

    /// The current HEAD body, ignoring any fault.
    pub fn peek_head(&self) -> Option<Vec<u8>> {
        self.lock().head.as_ref().map(|(b, _)| b.clone())
    }

    /// Every object name, ignoring any fault.
    pub fn object_names(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    /// Wait out a slow fault outside the lock and return the fault.
    fn fault(&self) -> Fault {
        let f = self.lock().fault;
        if let Fault::Slow(d) = f {
            std::thread::sleep(d);
        }
        f
    }
}

impl HeadStore for MemStore {
    fn read_head(&self) -> HeadRead {
        match self.fault() {
            Fault::Unreachable => Err(StoreError("unreachable".into())),
            Fault::AppliedReplyLost | Fault::RequestLost => Err(StoreError("timed out".into())),
            _ => Ok(self.lock().head.clone()),
        }
    }

    fn write_head(&self, body: &[u8], cond: &Cond) -> WriteOutcome {
        let fault = self.fault();
        match fault {
            Fault::Unreachable => return WriteOutcome::Unreachable,
            Fault::RequestLost => return WriteOutcome::Unknown,
            _ => {}
        }
        if body.contains(&b'\n') {
            return WriteOutcome::Unreachable;
        }
        let mut s = self.lock();
        let holds =
            fault == Fault::IgnoreConditions || cond_holds(cond, s.head.as_ref().map(|(_, e)| e));
        if !holds {
            return if fault == Fault::AppliedReplyLost {
                WriteOutcome::Unknown
            } else {
                WriteOutcome::PreconditionFailed
            };
        }
        s.version += 1;
        let etag = Etag(format!("\"{}\"", s.version));
        s.head = Some((body.to_vec(), etag.clone()));
        s.journal.push(body.to_vec());
        if fault == Fault::AppliedReplyLost {
            WriteOutcome::Unknown
        } else {
            WriteOutcome::Done(etag)
        }
    }
}

impl ObjectStore for MemStore {
    fn put_new(&self, name: &str, src: &Path) -> PutOutcome {
        let fault = self.fault();
        match fault {
            Fault::Unreachable => return PutOutcome::Unreachable,
            Fault::RequestLost => return PutOutcome::Unknown,
            _ => {}
        }
        let Ok(data) = std::fs::read(src) else {
            return PutOutcome::Unreachable;
        };
        let mut s = self.lock();
        if s.objects.contains_key(name) && fault != Fault::IgnoreConditions {
            return if fault == Fault::AppliedReplyLost {
                PutOutcome::Unknown
            } else {
                PutOutcome::PreconditionFailed
            };
        }
        s.objects.insert(name.to_string(), data);
        if fault == Fault::AppliedReplyLost {
            PutOutcome::Unknown
        } else {
            PutOutcome::Created
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        match self.fault() {
            Fault::Unreachable => Err(StoreError("unreachable".into())),
            Fault::AppliedReplyLost | Fault::RequestLost => Err(StoreError("timed out".into())),
            _ => Ok(self
                .lock()
                .objects
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect()),
        }
    }
}

/// A store in a local directory, safe for several processes on one host.
///
/// Layout under the root:
///
/// | Path | What |
/// | --- | --- |
/// | `lease/HEAD` | the ETag on the first line, the body on the second |
/// | `lease/journal.jsonl` | every applied HEAD body, one per line, append only |
/// | `lease/.lock` | taken with `File::lock` around every HEAD write |
/// | `<name>` | each object, created by a hard link so it appears whole or not at all |
#[derive(Debug, Clone)]
pub struct FsStore {
    root: PathBuf,
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// How much of the journal's end is read to mend it. A HEAD body is a few
/// kilobytes at most, so the last line always fits.
const MEND_TAIL: u64 = 1 << 20;

impl FsStore {
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(root.join("lease"))?;
        Ok(FsStore { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn head_path(&self) -> PathBuf {
        self.root.join("lease").join("HEAD")
    }

    pub fn journal_path(root: &Path) -> PathBuf {
        root.join("lease").join("journal.jsonl")
    }

    /// Every applied HEAD body under `root`, in order.
    pub fn read_journal(root: &Path) -> io::Result<Vec<Vec<u8>>> {
        let f = match File::open(Self::journal_path(root)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for line in BufReader::new(f).split(b'\n') {
            let line = line?;
            if !line.is_empty() {
                out.push(line);
            }
        }
        Ok(out)
    }

    fn lock(&self) -> io::Result<File> {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join("lease").join(".lock"))?;
        f.lock()?;
        Ok(f)
    }

    fn read_raw(&self) -> io::Result<Option<(Vec<u8>, Etag)>> {
        let mut buf = Vec::new();
        match File::open(self.head_path()) {
            Ok(mut f) => f.read_to_end(&mut buf)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
            return Err(io::Error::other("HEAD has no ETag line"));
        };
        let etag = String::from_utf8_lossy(&buf[..nl]).into_owned();
        Ok(Some((buf[nl + 1..].to_vec(), Etag(etag))))
    }

    /// Under the lock, make the journal end with the current HEAD body. A
    /// writer killed between its rename and its append left that applied
    /// write out of the journal, and one killed mid-append left a torn line
    /// that the next line would run into. Both are mended before a line is
    /// added, so the journal misses no applied write but the very last.
    fn mend_journal(&self, current: Option<&[u8]>) -> io::Result<()> {
        let mut j = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(Self::journal_path(&self.root))?;
        let len = j.metadata()?.len();
        let from = len.saturating_sub(MEND_TAIL);
        j.seek(SeekFrom::Start(from))?;
        let mut tail = Vec::new();
        j.read_to_end(&mut tail)?;
        let keep = match tail.iter().rposition(|&b| b == b'\n') {
            Some(i) => i + 1,
            None if from == 0 => 0,
            // One line longer than the tail: not a HEAD body, leave it.
            None => return Ok(()),
        };
        let mut changed = false;
        if keep < tail.len() {
            j.set_len(from + keep as u64)?;
            tail.truncate(keep);
            changed = true;
        }
        let last = tail[..keep.saturating_sub(1)]
            .rsplit(|&b| b == b'\n')
            .next()
            .filter(|l| !l.is_empty());
        if let Some(cur) = current {
            if last != Some(cur) {
                j.seek(SeekFrom::End(0))?;
                let mut line = cur.to_vec();
                line.push(b'\n');
                j.write_all(&line)?;
                changed = true;
            }
        }
        if changed {
            j.sync_data()?;
        }
        Ok(())
    }

    fn write_locked(&self, body: &[u8], cond: &Cond) -> io::Result<WriteOutcome> {
        let _lock = self.lock()?;
        let current = self.read_raw()?;
        if !cond_holds(cond, current.as_ref().map(|(_, e)| e)) {
            return Ok(WriteOutcome::PreconditionFailed);
        }
        self.mend_journal(current.as_ref().map(|(b, _)| b.as_slice()))?;
        let version = match &current {
            Some((_, Etag(e))) => e.trim_matches('"').parse::<u64>().unwrap_or(0) + 1,
            None => 1,
        };
        let etag = Etag(format!("\"{version}\""));
        let tmp = self.root.join("lease").join("HEAD.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(etag.0.as_bytes())?;
            f.write_all(b"\n")?;
            f.write_all(body)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, self.head_path())?;
        let mut j = OpenOptions::new()
            .create(true)
            .append(true)
            .open(Self::journal_path(&self.root))?;
        let mut line = body.to_vec();
        line.push(b'\n');
        j.write_all(&line)?;
        j.sync_data()?;
        Ok(WriteOutcome::Done(etag))
    }

    fn object_path(&self, name: &str) -> Option<PathBuf> {
        let p = Path::new(name);
        let ok = !name.is_empty()
            && !name.starts_with("lease/")
            && p.components().all(|c| matches!(c, Component::Normal(_)));
        ok.then(|| self.root.join(p))
    }

    fn walk(&self, dir: &Path, out: &mut Vec<String>) -> io::Result<()> {
        for e in std::fs::read_dir(dir)? {
            let e = e?;
            let path = e.path();
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') {
                continue;
            }
            if e.file_type()?.is_dir() {
                if path != self.root.join("lease") {
                    self.walk(&path, out)?;
                }
            } else if let Ok(rel) = path.strip_prefix(&self.root) {
                let parts: Vec<String> = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect();
                out.push(parts.join("/"));
            }
        }
        Ok(())
    }
}

impl HeadStore for FsStore {
    fn read_head(&self) -> HeadRead {
        self.read_raw().map_err(|e| StoreError(e.to_string()))
    }

    fn write_head(&self, body: &[u8], cond: &Cond) -> WriteOutcome {
        if body.contains(&b'\n') {
            return WriteOutcome::Unreachable;
        }
        // A failure before the rename leaves HEAD as it was; after it, the
        // write landed. Either way the caller cannot tell which.
        self.write_locked(body, cond)
            .unwrap_or(WriteOutcome::Unknown)
    }
}

impl ObjectStore for FsStore {
    fn put_new(&self, name: &str, src: &Path) -> PutOutcome {
        let Some(dst) = self.object_path(name) else {
            return PutOutcome::Unreachable;
        };
        let Some(dir) = dst.parent() else {
            return PutOutcome::Unreachable;
        };
        if std::fs::create_dir_all(dir).is_err() {
            return PutOutcome::Unreachable;
        }
        let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!(".put.{}.{n}", std::process::id()));
        if std::fs::copy(src, &tmp).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return PutOutcome::Unreachable;
        }
        // A hard link fails if the name exists, so the object appears whole
        // and only once.
        let out = match std::fs::hard_link(&tmp, &dst) {
            Ok(()) => PutOutcome::Created,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => PutOutcome::PreconditionFailed,
            Err(_) => PutOutcome::Unknown,
        };
        let _ = std::fs::remove_file(&tmp);
        out
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let mut out = Vec::new();
        self.walk(&self.root, &mut out)
            .map_err(|e| StoreError(e.to_string()))?;
        out.retain(|n| n.starts_with(prefix));
        out.sort();
        Ok(out)
    }
}

/// Check the semantics the lease and the archiver rely on, against a store
/// whose HEAD does not exist yet. `scratch` is a directory for one small
/// upload source file; object names are put under `prefix`.
pub fn contract_suite<S: HeadStore + ObjectStore>(
    s: &S,
    scratch: &Path,
    prefix: &str,
) -> Result<(), String> {
    let want = |ok: bool, what: &str| if ok { Ok(()) } else { Err(what.to_string()) };

    want(
        matches!(s.read_head(), Ok(None)),
        "a fresh store must have no HEAD",
    )?;
    want(
        s.write_head(b"{\"x\":0}", &Cond::IfMatch(Etag("\"nope\"".into())))
            == WriteOutcome::PreconditionFailed,
        "If-Match on a missing HEAD must fail",
    )?;
    let e1 = match s.write_head(b"{\"x\":1}", &Cond::IfNoneMatch) {
        WriteOutcome::Done(e) => e,
        o => return Err(format!("create with If-None-Match gave {o:?}")),
    };
    want(
        s.write_head(b"{\"x\":2}", &Cond::IfNoneMatch) == WriteOutcome::PreconditionFailed,
        "a second create must fail",
    )?;
    match s.read_head() {
        Ok(Some((b, e))) if b == b"{\"x\":1}" && e == e1 => {}
        r => return Err(format!("read after create gave {r:?}")),
    }
    let e2 = match s.write_head(b"{\"x\":3}", &Cond::IfMatch(e1.clone())) {
        WriteOutcome::Done(e) => e,
        o => return Err(format!("If-Match on the current ETag gave {o:?}")),
    };
    want(e2 != e1, "a write must change the ETag")?;
    want(
        s.write_head(b"{\"x\":4}", &Cond::IfMatch(e1)) == WriteOutcome::PreconditionFailed,
        "If-Match on a stale ETag must fail",
    )?;
    match s.read_head() {
        Ok(Some((b, e))) if b == b"{\"x\":3}" && e == e2 => {}
        r => return Err(format!("a refused write changed HEAD: {r:?}")),
    }

    std::fs::create_dir_all(scratch).map_err(|e| e.to_string())?;
    let src = scratch.join("contract-object");
    std::fs::write(&src, b"object").map_err(|e| e.to_string())?;
    let name = format!("{prefix}seg/e0000000001/s0000000001.prec.gz");
    want(
        s.put_new(&name, &src) == PutOutcome::Created,
        "the first put must create",
    )?;
    want(
        s.put_new(&name, &src) == PutOutcome::PreconditionFailed,
        "a second put of the same name must fail",
    )?;
    let listed = s.list(prefix).map_err(|e| e.to_string())?;
    want(listed == vec![name.clone()], "list must show the object")?;
    let other = s
        .list(&format!("{prefix}nothing/"))
        .map_err(|e| e.to_string())?;
    want(other.is_empty(), "list must filter by prefix")?;
    let _ = std::fs::remove_file(&src);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("ha-store-{}-{name}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn the_contract_holds_for_the_memory_store() {
        let d = scratch("mem");
        contract_suite(&MemStore::new(), &d, "selftest/").unwrap();
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn the_contract_holds_for_the_directory_store() {
        let d = scratch("fs");
        let s = FsStore::open(d.join("store")).unwrap();
        contract_suite(&s, &d, "").unwrap();
        // Every applied write is in the journal: the create and one update.
        let j = FsStore::read_journal(s.root()).unwrap();
        assert_eq!(j, vec![b"{\"x\":1}".to_vec(), b"{\"x\":3}".to_vec()]);
        let _ = std::fs::remove_dir_all(d);
    }

    fn write(s: &FsStore, body: &[u8]) {
        let cond = match s.read_head().unwrap() {
            Some((_, e)) => Cond::IfMatch(e),
            None => Cond::IfNoneMatch,
        };
        assert!(matches!(s.write_head(body, &cond), WriteOutcome::Done(_)));
    }

    #[test]
    fn the_journal_is_mended_before_the_next_write() {
        let d = scratch("mend");
        let s = FsStore::open(d.join("store")).unwrap();
        let jp = FsStore::journal_path(s.root());
        write(&s, b"aaaa");
        write(&s, b"bbbb");
        // A writer killed after its rename, before its append.
        std::fs::write(&jp, b"aaaa\n").unwrap();
        write(&s, b"cccc");
        write(&s, b"dddd");
        // A writer killed mid-append.
        std::fs::write(&jp, b"aaaa\nbbbb\ncccc\ndd").unwrap();
        write(&s, b"eeee");
        let want: Vec<Vec<u8>> = ["aaaa", "bbbb", "cccc", "dddd", "eeee"]
            .iter()
            .map(|l| l.as_bytes().to_vec())
            .collect();
        assert_eq!(FsStore::read_journal(s.root()).unwrap(), want);
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn the_contract_suite_catches_a_store_that_ignores_conditions() {
        let d = scratch("broken");
        let s = MemStore::new();
        s.set_fault(Fault::IgnoreConditions);
        assert!(contract_suite(&s, &d, "").is_err());
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn injected_faults_do_what_they_say() {
        let s = MemStore::new();
        s.set_fault(Fault::Unreachable);
        assert_eq!(
            s.write_head(b"a", &Cond::IfNoneMatch),
            WriteOutcome::Unreachable
        );
        assert!(s.read_head().is_err());
        assert!(s.peek_head().is_none(), "unreachable writes nothing");

        s.set_fault(Fault::RequestLost);
        assert_eq!(
            s.write_head(b"a", &Cond::IfNoneMatch),
            WriteOutcome::Unknown
        );
        assert!(s.peek_head().is_none(), "a lost request writes nothing");

        s.set_fault(Fault::AppliedReplyLost);
        assert_eq!(
            s.write_head(b"a", &Cond::IfNoneMatch),
            WriteOutcome::Unknown
        );
        assert_eq!(s.peek_head(), Some(b"a".to_vec()), "applied, reply lost");

        s.set_fault(Fault::Slow(Duration::from_millis(30)));
        let t = std::time::Instant::now();
        let r = s.read_head().unwrap().unwrap();
        assert!(t.elapsed() >= Duration::from_millis(30));
        assert_eq!(r.0, b"a".to_vec());
        assert_eq!(s.journal().len(), 1);
    }

    #[test]
    fn newlines_in_a_body_are_refused() {
        let s = MemStore::new();
        assert_eq!(
            s.write_head(b"a\nb", &Cond::IfNoneMatch),
            WriteOutcome::Unreachable
        );
        let d = scratch("nl");
        let f = FsStore::open(d.join("store")).unwrap();
        assert_eq!(
            f.write_head(b"a\nb", &Cond::IfNoneMatch),
            WriteOutcome::Unreachable
        );
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn object_names_cannot_escape_the_root_or_touch_the_lease() {
        let d = scratch("names");
        let f = FsStore::open(d.join("store")).unwrap();
        let src = d.join("src");
        std::fs::write(&src, b"x").unwrap();
        assert_eq!(f.put_new("../x", &src), PutOutcome::Unreachable);
        assert_eq!(f.put_new("lease/HEAD", &src), PutOutcome::Unreachable);
        assert_eq!(f.put_new("/abs", &src), PutOutcome::Unreachable);
        assert_eq!(f.put_new("seg/a", &src), PutOutcome::Created);
        assert_eq!(f.list("").unwrap(), vec!["seg/a".to_string()]);
        let _ = std::fs::remove_dir_all(d);
    }

    /// Several writers, each with its own handle on one directory, race
    /// read-then-write cycles. With the lock every applied write is based
    /// on the one before it, so no update is lost.
    #[test]
    fn racing_writers_on_one_directory_lose_no_update() {
        let d = scratch("race");
        let root = d.join("store");
        FsStore::open(&root).unwrap();
        let wins = Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let root = root.clone();
                let wins = wins.clone();
                std::thread::spawn(move || {
                    let s = FsStore::open(root).unwrap();
                    for _ in 0..50 {
                        let (n, cond) = match s.read_head().unwrap() {
                            None => (0u64, Cond::IfNoneMatch),
                            Some((b, e)) => (
                                String::from_utf8(b).unwrap().parse::<u64>().unwrap(),
                                Cond::IfMatch(e),
                            ),
                        };
                        let body = (n + 1).to_string();
                        if let WriteOutcome::Done(_) = s.write_head(body.as_bytes(), &cond) {
                            wins.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let s = FsStore::open(&root).unwrap();
        let (b, _) = s.read_head().unwrap().unwrap();
        let n: u64 = String::from_utf8(b).unwrap().parse().unwrap();
        assert_eq!(n, wins.load(Ordering::Relaxed));
        let j = FsStore::read_journal(&root).unwrap();
        assert_eq!(j.len() as u64, n);
        for (i, line) in j.iter().enumerate() {
            assert_eq!(line, (i as u64 + 1).to_string().as_bytes());
        }
        let _ = std::fs::remove_dir_all(d);
    }
}
