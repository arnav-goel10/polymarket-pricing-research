//! Soak test: stream a recording through the full pipeline at maximum speed,
//! looping, for a fixed time. Memory is sampled from outside (scripts/soak.sh).
//!
//! cargo run --release -p app --bin soak -- <file.prec> [--secs 600]

use app::replay_stream::Pipeline;
use feeds::recording::{ReadError, RecordReader};
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).ok_or("usage: soak <file.prec> [--secs N]")?;
    let secs: u64 = args
        .iter()
        .position(|a| a == "--secs")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let budget = Duration::from_secs(secs);
    let start = Instant::now();
    let mut pipeline = Pipeline::new();
    let (mut frames, mut passes) = (0u64, 0u64);
    let mut next_report = Duration::from_secs(10);
    'outer: loop {
        for rec in RecordReader::open(path)? {
            match rec {
                Ok(r) => {
                    std::hint::black_box(pipeline.on_record(&r));
                    frames += 1;
                }
                Err(ReadError::TruncatedTail { .. }) => break,
                Err(e) => return Err(e.into()),
            }
            if frames.is_multiple_of(4096) {
                let el = start.elapsed();
                if el >= next_report {
                    println!(
                        "t={:>4}s frames={frames} passes={passes} rate={:.0}/s",
                        el.as_secs(),
                        frames as f64 / el.as_secs_f64()
                    );
                    next_report += Duration::from_secs(10);
                }
                if el >= budget {
                    break 'outer;
                }
            }
        }
        passes += 1;
    }
    let el = start.elapsed().as_secs_f64();
    println!(
        "done: {frames} frames in {el:.1}s ({:.0}/s), {passes} full passes, repriced={} resyncs={}",
        frames as f64 / el,
        pipeline.repriced,
        pipeline.resyncs
    );
    Ok(())
}
