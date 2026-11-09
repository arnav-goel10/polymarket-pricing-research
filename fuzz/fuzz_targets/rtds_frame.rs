#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        for t in feeds::rtds::parse_frame(s) {
            let _ = (t.price, t.ts_millis);
        }
        let _ = feeds::rtds::parse_tick(s);
    }
});
