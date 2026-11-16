//! Snapshot size on the committed live recording: the whole file pushed
//! through a ring at the default caps, with a fresh client's snapshot
//! measured every 500 frames. Run with `--nocapture` to see the numbers.

use feeds::recording::read_all;
use ha::key::frame_keys;
use ha::ring::{Plan, Ring, RingConfig};
use ha::wire::Boot;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/live-sample.prec"
);

#[test]
fn snapshot_size_on_the_live_sample() {
    let (recs, tail) = read_all(FIXTURE).expect("read the live sample");
    assert_eq!(tail, None);
    // Default caps, and a ring a hundredth of that so the fixture overflows
    // it and the tail cap binds.
    for cfg in [
        RingConfig::default(),
        RingConfig {
            max_frames: 2_000,
            max_bytes: 671_088,
        },
    ] {
        let mut ring = Ring::new(Boot(1), cfg);
        let (cap_frames, cap_bytes) = ring.tail_cap();
        let mut keys = Vec::new();
        let (mut max_frames, mut max_bytes, mut incomplete, mut plans) = (0, 0, 0, 0);
        for (i, r) in recs.iter().enumerate() {
            keys.clear();
            let kind = frame_keys(r.source, &r.frame, &mut keys);
            ring.push(r.source, r.recv_micros, r.frame.as_str(), kind, &keys);
            if i % 500 != 499 {
                continue;
            }
            let Plan::Snapshot {
                frames, complete, ..
            } = ring.plan(None)
            else {
                panic!("a fresh client must get a snapshot");
            };
            let bytes: usize = frames.iter().map(|e| e.frame.len()).sum();
            max_frames = max_frames.max(frames.len());
            max_bytes = max_bytes.max(bytes);
            incomplete += usize::from(!complete);
            plans += 1;
            // Slots are at most one RTDS, two Binance and a book per asset.
            let slots = 3 + 16;
            assert!(
                frames.len() <= cap_frames + slots,
                "{} frames",
                frames.len()
            );
        }
        println!(
            "fixture snapshot, ring {} frames {} bytes: max {max_frames} frames {max_bytes} bytes, \
             {incomplete} of {plans} incomplete, tail cap {cap_frames} frames {cap_bytes} bytes, \
             ring now {} frames {} bytes",
            cfg.max_frames,
            cfg.max_bytes,
            ring.len(),
            ring.bytes()
        );
        assert!(plans > 0);
    }
}
