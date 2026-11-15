//! Count venue event keys in a recording and report collisions.
//!
//! keycheck <file.prec> [--fail-on-collision]
//!
//! Prints, per feed: frames, noise frames, events, events with a venue key,
//! unkeyed events, collisions (same key, different content) and repeats
//! (same key, same content). With `--fail-on-collision` it exits 1 if any
//! collision was found.

use ha::keycheck::check_file;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let fail_on_collision = args.iter().any(|a| a == "--fail-on-collision");
    let Some(path) = args.iter().find(|a| !a.starts_with("--")) else {
        eprintln!("usage: keycheck <file.prec> [--fail-on-collision]");
        return ExitCode::from(2);
    };
    match check_file(path) {
        Ok(report) => {
            println!("{path}");
            print!("{report}");
            if fail_on_collision && report.collisions() > 0 {
                eprintln!("keycheck: {} key collisions", report.collisions());
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("keycheck: {path}: {e}");
            ExitCode::from(2)
        }
    }
}
