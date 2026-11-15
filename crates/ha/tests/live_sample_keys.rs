//! Pins the key measurement on the committed live recording, so a change to
//! the key rules that introduces a collision, or silently stops keying a
//! frame type, fails here.

use ha::keycheck::{check_file, FeedCounts};
use ha::Feed;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/live-sample.prec"
);

fn counts(c: FeedCounts) -> (u64, u64, u64, u64, u64, u64) {
    (
        c.frames,
        c.events,
        c.keyed,
        c.unkeyed,
        c.collisions,
        c.repeats,
    )
}

#[test]
fn live_sample_has_no_key_collisions() {
    let r = check_file(FIXTURE).expect("read the live sample");
    assert_eq!(r.truncated_tail, None);
    assert_eq!(r.control_frames, 1);
    // (frames, events, keyed, unkeyed, collisions, repeats)
    assert_eq!(counts(r.feed(Feed::Rtds)), (5, 5, 5, 0, 0, 0));
    assert_eq!(counts(r.feed(Feed::Binance)), (5697, 5697, 5697, 0, 0, 0));
    assert_eq!(counts(r.feed(Feed::Clob)), (4238, 8389, 8389, 0, 0, 8));
    assert_eq!(r.collisions(), 0);
    // The first planned price change key collides on this same file.
    assert_eq!(r.plan_key_collisions, 298);
}
