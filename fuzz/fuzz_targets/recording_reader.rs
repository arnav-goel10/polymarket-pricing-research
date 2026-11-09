#![no_main]
//! Arbitrary bytes must never panic the reader, and whatever it accepts must
//! re-encode to exactly the bytes it was read from.
use feeds::recording::{read_bytes, RecordWriter, HEADER_LEN};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let (records, err) = read_bytes(data);
    if data.len() >= HEADER_LEN && !records.is_empty() {
        let mut w = RecordWriter::new(Vec::new()).unwrap();
        for r in &records {
            w.append(r.recv_micros, r.source, &r.frame).unwrap();
        }
        let bytes = w.into_inner().unwrap();
        assert_eq!(&bytes[HEADER_LEN..], &data[HEADER_LEN..bytes.len()]);
        if err.is_none() {
            assert_eq!(bytes.len(), data.len());
        }
    }
});
