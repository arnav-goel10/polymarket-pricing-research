//! Inspect or slice a recording.
//!
//! prec info  <file.prec>
//! prec slice <in.prec> <out.prec> --control <k> --max-bytes <n>
//!     Copy records starting at the k-th control (subscribe) record, so the
//!     slice begins with a fresh order-book subscription, until the output
//!     would exceed n bytes.

use feeds::recording::{read_all, RecordWriter, Source};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| -> Option<u64> {
        a.iter()
            .position(|x| x == name)
            .and_then(|i| a.get(i + 1))
            .and_then(|v| v.parse().ok())
    };
    match a.first().map(String::as_str) {
        Some("info") => {
            let path = a.get(1).ok_or("info <file>")?;
            let (recs, tail) = read_all(path)?;
            let bytes = std::fs::metadata(path)?.len();
            let span = match (recs.first(), recs.last()) {
                (Some(x), Some(y)) => (y.recv_micros - x.recv_micros) as f64 / 1e6,
                _ => 0.0,
            };
            let n = |s| recs.iter().filter(|r| r.source == s).count();
            println!(
                "{path}: {} records, {bytes} bytes, {span:.1} s, rtds={} binance={} clob_book={} control={}, truncated_tail={tail:?}",
                recs.len(),
                n(Source::Rtds),
                n(Source::Binance),
                n(Source::ClobBook),
                n(Source::Control)
            );
        }
        Some("slice") => {
            let (input, output) = (a.get(1).ok_or("in")?, a.get(2).ok_or("out")?);
            let k = flag("--control").unwrap_or(0) as usize;
            let max = flag("--max-bytes").ok_or("--max-bytes required")?;
            let (recs, _) = read_all(input)?;
            let start = recs
                .iter()
                .enumerate()
                .filter(|(_, r)| r.source == Source::Control)
                .nth(k)
                .map(|(i, _)| i)
                .ok_or("not enough control records")?;
            let mut w = RecordWriter::create(output)?;
            let mut size = feeds::recording::HEADER_LEN as u64;
            for r in &recs[start..] {
                let rec_len = 8 + 9 + r.frame.len() as u64;
                if size + rec_len > max {
                    break;
                }
                w.append(r.recv_micros, r.source, &r.frame)?;
                size += rec_len;
            }
            let (n, bytes) = w.finish()?;
            println!("wrote {n} records, {bytes} bytes to {output}");
        }
        _ => {
            return Err(
                "usage: prec info <file> | prec slice <in> <out> --control k --max-bytes n".into(),
            )
        }
    }
    Ok(())
}
