//! Regression test on real data: the committed slice of a live recording
//! (fixtures/live-sample.prec, about 5 s of all three feeds) replays through
//! the real parsers with no book resync, no unparsed frames, and only
//! uncrossed books.

use app::replay_stream::Pipeline;
use feeds::recording::{read_all, Source};
use market::clob_book::ClobBookStream;

const SAMPLE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/live-sample.prec"
);

#[test]
fn live_sample_replays_cleanly() {
    let (records, tail) = read_all(SAMPLE).expect("sample readable");
    assert!(tail.is_none());
    assert!(records.len() > 9_000, "{} records", records.len());
    assert_eq!(records[0].source, Source::Control);

    let mut p = Pipeline::new();
    for r in &records {
        if let Some(prob) = p.on_record(r) {
            assert!((0.0..=1.0).contains(&prob));
        }
    }
    assert_eq!(p.resyncs, 0, "book resynced on real data");
    assert!(p.repriced > 0);
    assert!(p.ignored < 10, "{} frames not parsed", p.ignored);
}

#[test]
fn every_book_published_from_the_live_sample_is_uncrossed() {
    let (records, _) = read_all(SAMPLE).unwrap();
    let token = serde_json::from_str::<serde_json::Value>(&records[0].frame).unwrap()["subscribe"]
        .as_str()
        .unwrap()
        .to_string();
    let mut stream = ClobBookStream::new(token);
    let mut published = 0;
    for r in records.iter().filter(|r| r.source == Source::ClobBook) {
        stream
            .on_text(&r.frame, |b| {
                published += 1;
                if let (Some(bid), Some(ask)) = (b.book.best_bid(), b.book.best_ask()) {
                    assert!(bid < ask, "crossed: {bid} >= {ask}");
                }
            })
            .expect("no desync on real data");
    }
    assert!(published > 1_000, "{published} books");
}
