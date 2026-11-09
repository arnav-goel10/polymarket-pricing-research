# Documentation

- [Architecture](architecture.md) — crate boundaries, runtime flow, persistence, and failure posture.
- [Design](DESIGN.md) — requirements, data flow, recording format, trade-offs and failure modes.
- [Runbook](RUNBOOK.md) — every `/metrics` signal, the alert it drives, and what to do.
- [Research methodology](research-methodology.md) — statistical unit, metrics, evidence labels, and acceptance criteria.
- [Research audit](research-audit.md) — reproducible report generated from the versioned shadow ledger.

Regenerate the audit and demo summary from the same source:

```bash
python3 scripts/analyze_shadow_strategy.py \
  --data fixtures/shadow-ledger-v1 \
  --report docs/research-audit.md \
  --json demo/summary.json
```
