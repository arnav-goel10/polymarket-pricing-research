# Documentation

- [Architecture](architecture.md) — crate boundaries, runtime flow, persistence, and failure posture.
- [Research methodology](research-methodology.md) — statistical unit, metrics, evidence labels, and acceptance criteria.
- [Research audit](research-audit.md) — reproducible report generated from the versioned shadow ledger.

Regenerate the audit and demo summary from the same source:

```bash
python3 scripts/analyze_shadow_strategy.py \
  --data fixtures/shadow-ledger-v1 \
  --report docs/research-audit.md \
  --json demo/summary.json
```
