# Publication Boundary

This repository is a paper-only research system. It may observe public market and reference feeds, but every downstream decision, fill, risk control, and ledger entry is simulated locally. No component can authenticate to an exchange or emit an external order.

| Capability | Public repository |
| --- | --- |
| Public market/reference data observation | Allowed |
| Pricing and calibration | Allowed |
| Decision generation | Simulation only |
| Fill and P&L accounting | Simulation only |
| Wallet/signing key | Absent |
| Authenticated order creation/cancellation | Absent |
| External order submission | Impossible by construction |

## Executable enforcement

[`scripts/check_paper_only.py`](../scripts/check_paper_only.py) scans every declared public capability surface (Rust crates, scripts, workflows, documentation, demo assets, fixtures, manifests, and environment templates) for credential, signer, exchange-SDK, and authenticated order-path indicators. CI fails with the file, line, and capability category for every finding.

The scanner deliberately allows paper-fill vocabulary and implementations such as `paper_engine.fill(decision)`: those operations mutate only the simulated portfolio and audit ledger. Scanner unit tests use adversarial temporary repositories and therefore remain outside the public capability scan roots.
