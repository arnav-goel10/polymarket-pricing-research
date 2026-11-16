//! The `chaos` binary's plan output and its refusal to run a seeded fault
//! in a build without the `mutants` feature.

use app::chaos::{FaultKind, Plan};
use std::process::Command;

#[test]
fn plan_prints_the_faults_it_will_run() {
    let out = Command::new(env!("CARGO_BIN_EXE_chaos"))
        .args(["plan", "--seed", "3", "--faults", "4", "--margin-s", "15"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let p: Plan = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(p.faults.len(), 4);
    assert_eq!(p.margin_ms, 15_000);
    assert_eq!(p.faults[0].kind, FaultKind::Kill9);
    assert_eq!(p.faults[1].kind, FaultKind::Freeze);
    assert!(!p.overlap);
}

#[test]
fn leader_freezes_take_the_last_faults() {
    let out = Command::new(env!("CARGO_BIN_EXE_chaos"))
        .args(["plan", "--faults", "3", "--leader-freezes", "2"])
        .args(["--leader-freeze-ms", "8000"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let p: Plan = serde_json::from_slice(&out.stdout).unwrap();
    let kinds: Vec<FaultKind> = p.faults.iter().map(|f| f.kind).collect();
    use FaultKind::*;
    assert_eq!(kinds, vec![Kill9, FreezeLeader, FreezeLeader]);
    assert_eq!(p.faults[2].freeze_ms, 8_000);
}

#[test]
fn unknown_commands_and_options_exit_2() {
    for args in [
        &["nope"][..],
        &["plan", "--seed"][..],
        &["verify"][..],
        &["verify-archive"][..],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_chaos"))
            .args(args)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{args:?}");
    }
}

#[cfg(not(feature = "mutants"))]
#[test]
fn a_build_without_mutants_refuses_a_seeded_fault() {
    for m in [
        "no-dedup",
        "seq-skip",
        "overlap-kill",
        "no-fence",
        "epoch-reuse",
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_chaos"))
            .args(["plan", "--mutant", m])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("refusing to start"), "{err}");
        assert!(out.stdout.is_empty());
    }
}

#[cfg(feature = "mutants")]
#[test]
fn a_mutants_build_plans_the_overlap_kill() {
    let out = Command::new(env!("CARGO_BIN_EXE_chaos"))
        .args(["plan", "--faults", "2", "--mutant", "overlap-kill"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let p: Plan = serde_json::from_slice(&out.stdout).unwrap();
    assert!(p.overlap);
    assert!(p.faults.iter().all(|f| f.kind == FaultKind::Kill9));
}
