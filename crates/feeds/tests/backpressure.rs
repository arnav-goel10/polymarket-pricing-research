//! A stalled consumer cannot make the recorder queue grow memory.
//!
//! A counting global allocator tracks live heap bytes for this test binary.
//! Producers push 200,000 frames of 1 KiB (about 200 MB) into a queue of
//! 1,024 slots while the writer is not running. Live heap must stay near
//! 1,024 frames and every excess frame must be counted as dropped.

use feeds::recorder::queue;
use feeds::recording::{read_bytes, RecordWriter, Source};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering::Relaxed};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size() as isize, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as isize, Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        LIVE.fetch_add(new as isize - l.size() as isize, Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static A: Counting = Counting;

#[test]
fn stalled_consumer_keeps_memory_bounded_and_counts_drops() {
    const CAP: usize = 1_024;
    const FRAMES: usize = 200_000;
    let frame = "x".repeat(1_024);

    let base = LIVE.load(Relaxed);
    let (handle, stalled) = queue(CAP);
    let mut peak = 0isize;
    for i in 0..FRAMES {
        handle.record_at(i as i64, Source::ClobBook, &frame);
        if i % 1_000 == 0 {
            peak = peak.max(LIVE.load(Relaxed) - base);
        }
    }
    peak = peak.max(LIVE.load(Relaxed) - base);
    let s = &handle.stats;
    assert_eq!(s.enqueued.load(Relaxed), CAP as u64);
    assert_eq!(s.dropped.load(Relaxed), (FRAMES - CAP) as u64);
    // 1,024 queued frames of ~1 KiB plus channel overhead: well under 4 MiB,
    // against ~200 MiB offered.
    let bound = 4 * 1024 * 1024;
    println!("peak live heap growth: {peak} bytes for {FRAMES} frames offered (bound {bound})");
    assert!(peak < bound, "queue grew to {peak} bytes");

    // Once the consumer starts, it writes exactly the queued frames.
    let stats = handle.stats.clone();
    drop(handle);
    let task = stalled.start(RecordWriter::new(Vec::new()).unwrap(), |w| {
        let bytes = w.into_inner()?;
        let (recs, err) = read_bytes(&bytes);
        assert!(err.is_none());
        assert_eq!(recs.len(), CAP);
        assert_eq!(recs[CAP - 1].recv_micros, (CAP - 1) as i64);
        Ok((recs.len() as u64, bytes.len() as u64))
    });
    task.finish().unwrap();
    assert_eq!(stats.queue_depth(), 0);
}
