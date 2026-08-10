//! Deterministic offline replay for pricing-model evaluation.

use app::backtest::{score_models, Sample};
use poly_core::ReplaySample;
use pricing::{BlackScholesDigital, CosVgDigital, DigitalModel, SharpDigital, StudentTDigital};
use serde::Deserialize;
use serde_json::json;
use std::error::Error;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct ReplayDataset {
    dataset_version: String,
    description: String,
    samples: Vec<ReplaySample>,
}

fn probability_metrics(values: impl Iterator<Item = (f64, bool)>) -> (f64, f64, usize) {
    let mut brier = 0.0;
    let mut log_loss = 0.0;
    let mut count = 0;
    for (probability, outcome) in values {
        let probability = probability.clamp(1e-9, 1.0 - 1e-9);
        let target = f64::from(outcome);
        brier += (probability - target).powi(2);
        log_loss -= target * probability.ln() + (1.0 - target) * (1.0 - probability).ln();
        count += 1;
    }
    if count == 0 {
        return (0.0, 0.0, 0);
    }
    (brier / count as f64, log_loss / count as f64, count)
}

fn load_dataset(path: &Path) -> Result<ReplayDataset, Box<dyn Error>> {
    let dataset: ReplayDataset = serde_json::from_slice(&std::fs::read(path)?)?;
    if dataset.samples.is_empty() {
        return Err("replay dataset must contain at least one sample".into());
    }
    for (index, sample) in dataset.samples.iter().enumerate() {
        let context = sample.ctx;
        if !context.s_t.is_finite()
            || !context.s0.is_finite()
            || context.s_t <= 0.0
            || context.s0 <= 0.0
            || !context.tau.is_finite()
            || context.tau <= 0.0
            || !context.sigma.is_finite()
            || context.sigma <= 0.0
        {
            return Err(format!("sample {index} has an invalid pricing context").into());
        }
        for probability in [sample.best_bid, sample.best_ask, sample.fill_price]
            .into_iter()
            .flatten()
        {
            if !(0.0..=1.0).contains(&probability) {
                return Err(format!("sample {index} has an invalid market probability").into());
            }
        }
    }
    Ok(dataset)
}

fn evaluate(dataset: &ReplayDataset) -> serde_json::Value {
    let samples: Vec<Sample> = dataset
        .samples
        .iter()
        .map(|sample| Sample {
            ctx: sample.ctx,
            outcome_up: sample.outcome_up,
        })
        .collect();
    let models: Vec<Box<dyn DigitalModel>> = vec![
        Box::new(BlackScholesDigital),
        Box::new(StudentTDigital { nu: 4.0 }),
        Box::new(SharpDigital::new(2.5)),
        Box::new(CosVgDigital::new(64)),
    ];
    let mut scores = score_models(&models, &samples);
    scores.sort_by(|left, right| left.brier.total_cmp(&right.brier));

    let (market_brier, market_log_loss, market_samples) =
        probability_metrics(dataset.samples.iter().filter_map(|sample| {
            Some((
                (sample.best_bid? + sample.best_ask?) / 2.0,
                sample.outcome_up,
            ))
        }));
    let model_rows: Vec<_> = scores
        .iter()
        .map(|score| {
            json!({
                "name": score.name,
                "brier": score.brier,
                "log_loss": score.log_loss,
                "brier_lift_vs_market": market_brier - score.brier,
            })
        })
        .collect();

    json!({
        "dataset_version": dataset.dataset_version,
        "description": dataset.description,
        "samples": dataset.samples.len(),
        "market_baseline": {
            "samples": market_samples,
            "brier": market_brier,
            "log_loss": market_log_loss,
        },
        "models": model_rows,
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let path = args
        .iter()
        .find(|arg| !arg.starts_with("--"))
        .map(String::as_str)
        .unwrap_or("fixtures/replay-sample.json");
    let result = evaluate(&load_dataset(Path::new(path))?);
    if args.iter().any(|arg| arg == "--json") {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("Poly deterministic replay");
        println!(
            "dataset: {}",
            result["dataset_version"].as_str().unwrap_or("unknown")
        );
        println!("samples: {}", result["samples"]);
        println!(
            "market Brier: {:.5}",
            result["market_baseline"]["brier"].as_f64().unwrap_or(0.0)
        );
        for model in result["models"].as_array().into_iter().flatten() {
            println!(
                "{:<16} Brier {:.5}  log-loss {:.5}  lift vs market {:+.5}",
                model["name"].as_str().unwrap_or("unknown"),
                model["brier"].as_f64().unwrap_or(0.0),
                model["log_loss"].as_f64().unwrap_or(0.0),
                model["brier_lift_vs_market"].as_f64().unwrap_or(0.0),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_replays_deterministically() {
        let fixture = include_str!("../../../../fixtures/replay-sample.json");
        let dataset: ReplayDataset = serde_json::from_str(fixture).unwrap();
        let first = evaluate(&dataset);
        let second = evaluate(&dataset);
        assert_eq!(first, second);
        assert_eq!(first["samples"], dataset.samples.len());
        assert_eq!(first["models"].as_array().unwrap().len(), 4);
    }
}
