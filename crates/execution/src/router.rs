use crate::edge::{Side, TradeDecision};
use poly_core::{ExecutionConfig, RunMode};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;

/// Paper-execution mode. Neither variant can sign or submit an exchange order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ExecMode {
    #[default]
    Shadow,
    DryRun,
}

impl From<RunMode> for ExecMode {
    fn from(value: RunMode) -> Self {
        match value {
            RunMode::Shadow => ExecMode::Shadow,
            RunMode::DryRun => ExecMode::DryRun,
        }
    }
}

/// A simulated fill used for evaluation, calibration, and restart recovery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillRecord {
    pub idempotency_key: String,
    #[serde(default)]
    pub mode: ExecMode,
    pub side: Side,
    #[serde(default)]
    pub requested_size: f64,
    #[serde(default)]
    pub requested_notional: f64,
    pub size: f64,
    pub price: f64,
    #[serde(default)]
    pub matched_size: f64,
    #[serde(default)]
    pub matched_notional: f64,
    #[serde(default)]
    pub fill_status: String,
    pub ts_millis: i64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub idempotency_key: String,
    pub mode: ExecMode,
    pub status: String,
    pub matched_size: f64,
    pub matched_notional: f64,
    pub error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("persistence error: {0}")]
    Persist(String),
}

#[derive(Debug, Default)]
pub struct ShadowRecorder {
    fills: Vec<FillRecord>,
}

impl ShadowRecorder {
    pub fn record(&mut self, mode: ExecMode, decision: &TradeDecision, ts_millis: i64) {
        let status = match mode {
            ExecMode::Shadow => "shadow",
            ExecMode::DryRun => "dry-run",
        };
        self.fills.push(FillRecord {
            idempotency_key: idempotency_key(ts_millis, decision),
            mode,
            side: decision.side,
            requested_size: decision.size,
            requested_notional: decision.size * decision.vwap,
            size: decision.size,
            price: decision.vwap,
            matched_size: decision.size,
            matched_notional: decision.size * decision.vwap,
            fill_status: "simulated".into(),
            ts_millis,
            status: status.into(),
        });
    }

    pub fn fills(&self) -> &[FillRecord] {
        &self.fills
    }

    pub fn notional(&self) -> f64 {
        self.fills.iter().map(|fill| fill.size * fill.price).sum()
    }
}

/// Records paper fills only. The service has no signer, credential, or order-posting API.
pub struct ExecutionService {
    mode: ExecMode,
    cfg: ExecutionConfig,
    recorder: ShadowRecorder,
    recent_decision_ts: VecDeque<i64>,
    last_error: Option<String>,
}

impl ExecutionService {
    pub fn new(mode: ExecMode, cfg: ExecutionConfig) -> Self {
        Self {
            mode,
            cfg,
            recorder: ShadowRecorder::default(),
            recent_decision_ts: VecDeque::new(),
            last_error: None,
        }
    }

    pub fn mode(&self) -> ExecMode {
        self.mode
    }

    pub fn shadow_fills(&self) -> usize {
        self.recorder.fills().len()
    }

    pub fn notional(&self) -> f64 {
        self.recorder.notional()
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn decision_count_since(&mut self, now_millis: i64, window_millis: i64) -> u32 {
        while self
            .recent_decision_ts
            .front()
            .is_some_and(|ts| now_millis - *ts > window_millis)
        {
            self.recent_decision_ts.pop_front();
        }
        self.recent_decision_ts.len() as u32
    }

    pub async fn execute(
        &mut self,
        _token_id: &str,
        decision: &TradeDecision,
        ts_millis: i64,
    ) -> Result<ExecutionReport, ExecutionError> {
        let idempotency_key = idempotency_key(ts_millis, decision);
        self.recorder.record(self.mode, decision, ts_millis);
        if let Err(error) = self.persist_latest() {
            self.last_error = Some(error.to_string());
            return Err(error);
        }
        self.recent_decision_ts.push_back(ts_millis);
        Ok(ExecutionReport {
            idempotency_key,
            mode: self.mode,
            status: match self.mode {
                ExecMode::Shadow => "shadow-simulated",
                ExecMode::DryRun => "dry-run-simulated",
            }
            .into(),
            matched_size: decision.size,
            matched_notional: decision.size * decision.vwap,
            error: None,
        })
    }

    fn persist_latest(&self) -> Result<(), ExecutionError> {
        let Some(fill) = self.recorder.fills().last() else {
            return Ok(());
        };
        if self.cfg.persist_fills_path.is_empty() {
            return Ok(());
        }
        let line = serde_json::to_string(fill)
            .map_err(|error| ExecutionError::Persist(error.to_string()))?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.cfg.persist_fills_path)
            .map_err(|error| ExecutionError::Persist(error.to_string()))?;
        writeln!(file, "{line}").map_err(|error| ExecutionError::Persist(error.to_string()))
    }
}

fn idempotency_key(ts_millis: i64, decision: &TradeDecision) -> String {
    format!(
        "{ts_millis}-{:?}-{:.6}-{:.6}-{:.6}",
        decision.side, decision.size, decision.vwap, decision.edge
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use poly_core::DryRunStrategy;

    fn cfg() -> ExecutionConfig {
        ExecutionConfig {
            token_id_override: None,
            persist_fills_path: String::new(),
            dry_run_strategy: DryRunStrategy::HoldSharp,
            latency_edge_buffer: 0.02,
            min_profit_buffer: 0.10,
        }
    }

    fn decision() -> TradeDecision {
        TradeDecision {
            side: Side::Buy,
            size: 2.0,
            vwap: 0.6,
            edge: 0.1,
        }
    }

    #[test]
    fn mode_default_is_shadow() {
        assert_eq!(ExecMode::default(), ExecMode::Shadow);
    }

    #[tokio::test]
    async fn shadow_records_a_simulated_fill() {
        let mut service = ExecutionService::new(ExecMode::Shadow, cfg());
        let report = service.execute("unused", &decision(), 1_000).await.unwrap();
        assert_eq!(report.status, "shadow-simulated");
        assert_eq!(report.matched_notional, 1.2);
        assert_eq!(service.shadow_fills(), 1);
    }

    #[tokio::test]
    async fn dry_run_cannot_submit_an_external_order() {
        let mut service = ExecutionService::new(ExecMode::DryRun, cfg());
        let report = service.execute("unused", &decision(), 1_000).await.unwrap();
        assert_eq!(report.status, "dry-run-simulated");
        assert_eq!(report.mode, ExecMode::DryRun);
        assert!(report.error.is_none());
        assert_eq!(service.shadow_fills(), 1);
    }
}
