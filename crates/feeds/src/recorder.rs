//! Non-blocking recorder: feed tasks hand raw frames to a bounded queue and a
//! dedicated writer thread appends them to a recording file.
//!
//! Slow-consumer policy: **drop the newest frame and count it.** The queue
//! holds at most `capacity` frames. When the disk (the consumer) falls
//! behind, [`RecorderHandle::record`] never blocks or allocates beyond that
//! bound; it discards the incoming frame and increments `dropped`. A feed task
//! must never stall on disk I/O, because a stalled socket reader is what makes
//! an exchange connection go stale. Every gap is visible in
//! `poly_recorder_dropped_total`, so a recording with drops is known to be
//! incomplete rather than silently wrong.
//!
//! Shutdown: drop every [`RecorderHandle`]; the writer drains what is queued,
//! flushes, fsyncs and returns the totals from [`RecorderTask::finish`].

use crate::recording::{RecordWriter, Source};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;

/// Counters shared between producers and the `/metrics` endpoint.
#[derive(Debug, Default)]
pub struct RecorderStats {
    pub enqueued: AtomicU64,
    pub dropped: AtomicU64,
    pub written: AtomicU64,
    pub capacity: AtomicU64,
}

impl RecorderStats {
    /// Frames accepted but not yet written.
    pub fn queue_depth(&self) -> u64 {
        self.enqueued
            .load(Relaxed)
            .saturating_sub(self.written.load(Relaxed))
    }
}

struct Frame {
    recv_micros: i64,
    source: Source,
    text: String,
}

/// Cheap to clone; one per feed task.
#[derive(Clone)]
pub struct RecorderHandle {
    tx: SyncSender<Frame>,
    pub stats: Arc<RecorderStats>,
}

impl RecorderHandle {
    /// Queue one frame. Returns `false` if it was dropped (queue full or
    /// writer gone). Never blocks.
    pub fn record(&self, source: Source, frame: &str) -> bool {
        self.record_at(poly_core::metrics::wall_millis() * 1000, source, frame)
    }

    pub fn record_at(&self, recv_micros: i64, source: Source, frame: &str) -> bool {
        let f = Frame {
            recv_micros,
            source,
            text: frame.to_owned(),
        };
        match self.tx.try_send(f) {
            Ok(()) => {
                self.stats.enqueued.fetch_add(1, Relaxed);
                true
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.stats.dropped.fetch_add(1, Relaxed);
                false
            }
        }
    }
}

/// The writer side. Hold it until shutdown, then call [`finish`](Self::finish).
pub struct RecorderTask {
    join: JoinHandle<std::io::Result<(u64, u64)>>,
    pub stats: Arc<RecorderStats>,
}

impl RecorderTask {
    /// Wait for the writer to drain the queue, flush and fsync. Call after
    /// every handle has been dropped, or this waits forever.
    pub fn finish(self) -> std::io::Result<(u64, u64)> {
        self.join
            .join()
            .unwrap_or_else(|_| Err(std::io::Error::other("recorder thread panicked")))
    }
}

/// A bounded queue whose consumer has not started yet. Splitting creation
/// from [`QueueOnly::start`] lets tests hold the consumer stalled.
pub struct QueueOnly {
    rx: Receiver<Frame>,
    pub stats: Arc<RecorderStats>,
}

pub fn queue(capacity: usize) -> (RecorderHandle, QueueOnly) {
    let (tx, rx) = sync_channel(capacity);
    let stats = Arc::new(RecorderStats::default());
    stats.capacity.store(capacity as u64, Relaxed);
    (
        RecorderHandle {
            tx,
            stats: stats.clone(),
        },
        QueueOnly { rx, stats },
    )
}

impl QueueOnly {
    /// Start the writer thread appending to `writer`.
    pub fn start<W: std::io::Write + Send + 'static>(
        self,
        mut writer: RecordWriter<W>,
        on_done: impl FnOnce(RecordWriter<W>) -> std::io::Result<(u64, u64)> + Send + 'static,
    ) -> RecorderTask {
        let stats = self.stats.clone();
        let rx = self.rx;
        let join = std::thread::Builder::new()
            .name("recorder".into())
            .spawn(move || {
                // Runs until every sender is dropped and the queue is empty.
                while let Ok(f) = rx.recv() {
                    writer.append(f.recv_micros, f.source, &f.text)?;
                    stats.written.fetch_add(1, Relaxed);
                }
                on_done(writer)
            })
            .expect("spawn recorder thread");
        RecorderTask {
            join,
            stats: self.stats,
        }
    }
}

/// Open `path` and start recording with a queue of `capacity` frames.
pub fn start_file(
    path: impl AsRef<Path>,
    capacity: usize,
) -> std::io::Result<(RecorderHandle, RecorderTask)> {
    let writer = RecordWriter::create(path)?;
    let (handle, q) = queue(capacity);
    Ok((handle, q.start(writer, |w| w.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recording::read_bytes;

    #[test]
    fn drains_everything_queued_on_shutdown() {
        let (h, q) = queue(10_000);
        let task = q.start(RecordWriter::new(Vec::new()).unwrap(), |w| {
            let n = w.records();
            let bytes = w.into_inner()?;
            let (recs, err) = read_bytes(&bytes);
            assert!(err.is_none());
            assert_eq!(recs.len() as u64, n);
            Ok((n, bytes.len() as u64))
        });
        for i in 0..5_000 {
            assert!(h.record_at(i, Source::Binance, "x"));
        }
        let stats = h.stats.clone();
        drop(h);
        let (n, _) = task.finish().unwrap();
        assert_eq!(n, 5_000);
        assert_eq!(stats.dropped.load(Relaxed), 0);
        assert_eq!(stats.written.load(Relaxed), 5_000);
    }

    #[test]
    fn full_queue_drops_newest_and_counts() {
        let (h, q) = queue(4);
        for i in 0..10 {
            h.record_at(i, Source::Rtds, "f");
        }
        assert_eq!(h.stats.enqueued.load(Relaxed), 4);
        assert_eq!(h.stats.dropped.load(Relaxed), 6);
        let stats = h.stats.clone();
        drop(h);
        let task = q.start(RecordWriter::new(Vec::new()).unwrap(), |w| {
            let bytes = w.into_inner()?;
            let (recs, _) = read_bytes(&bytes);
            // The four oldest frames survive; the newest six were dropped.
            assert_eq!(
                recs.iter().map(|r| r.recv_micros).collect::<Vec<_>>(),
                vec![0, 1, 2, 3]
            );
            Ok((4, bytes.len() as u64))
        });
        task.finish().unwrap();
        assert_eq!(stats.queue_depth(), 0);
    }
}
