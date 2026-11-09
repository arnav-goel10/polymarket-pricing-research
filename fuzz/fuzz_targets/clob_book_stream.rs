#![no_main]
//! Seed a valid book, then feed fuzzed frames (split on newlines). Beyond
//! "no panic", every published book must be sorted best-first and uncrossed.
use libfuzzer_sys::fuzz_target;
use market::clob_book::ClobBookStream;

const SNAP: &str = r#"{"event_type":"book","asset_id":"tok","timestamp":"1","bids":[{"price":"0.48","size":"10"},{"price":"0.47","size":"5"}],"asks":[{"price":"0.52","size":"10"},{"price":"0.53","size":"5"}]}"#;

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else { return };
    let mut stream = ClobBookStream::new("tok");
    stream.on_text(SNAP, |_| {}).unwrap();
    for frame in s.split('\n') {
        let res = stream.on_text(frame, |b| {
            for w in b.book.bids.windows(2) {
                assert!(w[0].price > w[1].price, "bids unsorted");
            }
            for w in b.book.asks.windows(2) {
                assert!(w[0].price < w[1].price, "asks unsorted");
            }
            if let (Some(bid), Some(ask)) = (b.book.best_bid(), b.book.best_ask()) {
                assert!(bid < ask, "published crossed book");
            }
        });
        if res.is_err() {
            // Desync: a real client reconnects with a fresh stream.
            stream = ClobBookStream::new("tok");
            stream.on_text(SNAP, |_| {}).unwrap();
        }
    }
});
