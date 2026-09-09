# EvidenceHunter

## An AI-assisted market observation and evidence evaluation framework built around research-grade practices

**EvidenceHunter is also an experiment in AI-native software development: can someone without a traditional software engineering background use AI coding agents to independently build, verify, document, and maintain a complex research software system?**

EvidenceHunter focuses on systematic market observation, evidence collection, provenance, validation, reproducibility, and explicit research governance.

It is not designed as an automated trading system or financial advice tool.

---

## Why EvidenceHunter Exists

This project began with two connected questions.

### Research question

Can market observations be collected and evaluated through a workflow that keeps evidence, assumptions, provenance, and conclusions clearly separated?

### Engineering question

Can a person without formal software engineering training use modern AI coding agents as engineering collaborators while still maintaining:

- automated testing
- reproducible workflows
- explicit governance
- dataset lifecycle controls
- documented research decisions
- version-controlled development
- continuous integration

EvidenceHunter is an attempt to explore both questions through a real, continuously maintained software project.

---

## What EvidenceHunter Does

The current framework includes components for:

- market data collection
- order book and order flow observation
- dataset management
- gap and completeness tracking
- archive materialization and verification
- feature construction
- shadow outcome analysis
- discovery experiments
- provenance tracking
- research readiness checks
- governance and implementation-state tracking

The emphasis is not on producing confident predictions.

The emphasis is on making the path from **observation → evidence → evaluation → conclusion** inspectable.

---

## What EvidenceHunter Is Not

EvidenceHunter is not:

- a guaranteed-profit system
- a price prediction service
- financial advice
- an autonomous trading bot
- a substitute for independent research or risk assessment

---

## AI-Assisted Development

AI coding agents have been used extensively throughout the development of EvidenceHunter.

AI is treated as an engineering collaborator rather than an unquestioned authority.

The development process emphasizes:

- evidence before claims
- verification before declaring work complete
- tests after meaningful code changes
- explicit scope and implementation records
- documented failures and corrections
- human-controlled decisions
- iterative review of AI-generated work

The repository intentionally preserves governance artifacts and research records because the development process itself is part of the experiment.

See:

`docs/AI_DEVELOPMENT_STORY.md`

`docs/ARCHITECTURE.md`

`docs/RESEARCH_METHOD.md`

`AGENTS.md`

---

## Evidence of Engineering Discipline

The public repository includes:

### Automated tests

The `tests/` directory contains regression, integration, lifecycle, collector, archive, dataset, and research-workflow tests.

### Continuous integration

GitHub Actions automatically runs the Python test suite on changes to the repository.

The current public baseline passes CI on Windows.

### Research governance

`research/governance/` contains artifacts for:

- scope declarations
- implementation handoffs
- change roles
- readiness reports
- test results
- reconciliation
- execution contracts

### Dataset lifecycle controls

`dataset_lifecycle.json` and related modules track dataset state and help prevent accidental reuse or mutation of frozen research datasets.

### Provenance and reproducibility

The repository includes provenance, summary, readiness, continuity, and cross-platform verification artifacts for research executions.

---

## Architecture

EvidenceHunter is organized around several layers.

### Collection Layer

Collects and records structured market observations.

### Evidence Layer

Preserves observations, provenance, completeness information, and dataset state.

### Evaluation Layer

Supports feature construction, shadow outcomes, discovery experiments, and structured comparisons.

### Governance Layer

Tracks scope, implementation decisions, readiness, testing, and research-state transitions.

More detail is available in:

`docs/ARCHITECTURE.md`

---

## Repository Structure

```text
EvidenceHunter/
│
├── EvidenceHunter_*.py
├── audit_order.py
├── governance_guard.py
├── dataset_lifecycle.json
│
├── tests/
│
├── research/
│   └── governance/
│
├── research_design_next/
│
├── docs/
│   ├── AI_DEVELOPMENT_STORY.md
│   ├── ARCHITECTURE.md
│   └── RESEARCH_METHOD.md
│
├── AGENTS.md
├── CONTRIBUTING.md
├── requirements-collector.txt
└── README.md