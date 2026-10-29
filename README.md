# Spyne Monthly Revenue Agent Squad

A resilient, multi-agent data engineering and financial verification squad that produces a monthly NYC Yellow Taxi revenue summary by pickup zone and payment type, detects and adapts to upstream schema/domain drift (`2024` vs. `2025`), enforces strict verifier independence, maintains crash-safe durable state in SQLite, and refuses to ship rather than outputting a wrong number.

## Quick Start (Runs in < 2 Minutes)

### 1. Environment Setup

Requires **Python 3.10+** with `pyarrow`, `pandas`, and `pydantic` (no API keys or login required):

```bash
pip install -r requirements.txt
```

### 2. One-Command Execution Against Any Named Month

Run the squad end-to-end against any month (`YYYY-MM`). If the Parquet file is not already cached in `data/`, `run.py` automatically downloads `yellow_tripdata_<YYYY-MM>.parquet` and `taxi_zone_lookup.csv` from the official [NYC TLC CloudFront distribution](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):

```bash
# 1. Clean Pass against 2024 baseline month (19 columns):
python3 run.py --month 2024-01 --reset

# 2. Drift Caught & Handled + Verifier Repair Loop against 2025 month (20 columns, cbd_congestion_fee, Flex Fare):
python3 run.py --month 2025-01 --reset

# 3. Refusal Path against corrupted / contract-violating source data (exits with code 1 and refuses to ship):
python3 run.py --month 2025-99 --reset

# 4. Run against any unseen month (e.g., 2024-06 or 2025-02):
python3 run.py --month 2025-02
```

Positional syntax (`python3 run.py 2025-01`) and explicit file override (`python3 run.py --month 2025-01 --parquet-path /path/to/file.parquet`) are also supported.

### 3. Test Durable State (Mid-Run Process Kill & Resume Without Redoing Work)

```bash
# Step A: Start a run and simulate a process crash (SIGKILL / exit 137) right after Stage 1 (Profiling & Drift Detection):
python3 run.py --month 2025-01 --run-id demo_resume --reset --simulate-kill-after STAGE_1_PROFILE_AND_DRIFT

# Step B: Re-run the exact same run-id without --reset.
# The orchestrator loads STAGE_1_PROFILE_AND_DRIFT from state/squad_state.db without re-profiling:
python3 run.py --month 2025-01 --run-id demo_resume
```

### 4. Run the Full Automated Verification & Unit Test Suite

```bash
python3 tests/test_squad.py
```

## Architecture & Repository Layout

```text
spyne-revenue-squad/
├── run.py                                      # Single-command CLI entrypoint
├── DESIGN_NOTE.md                              # 2-page technical design note (Markdown)
├── DESIGN_NOTE.pdf                             # 2-page technical design note (PDF)
├── requirements.txt                            # Python dependencies
├── contracts/
│   ├── schema_baseline_2024.json               # 2024 baseline schema & adaptable extensions contract
│   └── revenue_contract.json                   # Formal Finance revenue definition & verifier thresholds
├── squad/
│   ├── models.py                               # Pydantic contracts & VerifierInput isolation firewall
│   ├── state_store.py                          # Durable SQLite checkpoint & audit trace store
│   ├── sql_engine.py                           # Real read-only SQL & PyArrow Parquet profiling tools
│   ├── data_loader.py                          # Automatic NYC TLC CloudFront downloader & cache manager
│   ├── profiler_agent.py                       # Agent 1: SourceProfilerAndDriftAgent
│   ├── builder_agent.py                        # Agent 2: SQLRevenueBuilderAgent (with repair loop)
│   ├── verifier_agent.py                       # Agent 3: IndependentVerifierAgent (isolated V1..V8 audits)
│   └── orchestrator.py                         # RevenueSquadOrchestrator (bounds, retries, durability)
├── data/
│   ├── taxi_zone_lookup.csv                    # Official 265 NYC TLC Taxi Zones
│   ├── yellow_tripdata_2024-01.parquet         # 2024-01 dataset (19 columns)
│   ├── yellow_tripdata_2025-01.parquet         # 2025-01 dataset (20 columns, cbd_congestion_fee, Flex Fare)
│   └── yellow_tripdata_2025-99.parquet         # Corrupted dataset for refusal verification
├── outputs/
│   ├── revenue_summary_2024-01.csv             # Final verified 2024-01 revenue summary (CSV)
│   ├── revenue_summary_2024-01.json            # Final verified 2024-01 revenue summary (JSON)
│   ├── verifier_evidence_2024-01.json          # Independent Verifier acceptance evidence for 2024-01
│   ├── revenue_summary_2025-01.csv             # Final verified 2025-01 revenue summary (CSV)
│   ├── revenue_summary_2025-01.json            # Final verified 2025-01 revenue summary (JSON)
│   ├── verifier_evidence_2025-01_attempt_1.json# Verifier rejection of Attempt 1 on 2025-01 (V4 failure)
│   ├── verifier_evidence_2025-01.json          # Verifier acceptance of Attempt 2 on 2025-01 (8/8 checks)
│   └── refusal_report_2025-99.json             # Formal refusal & escalation report for 2025-99
├── traces/
│   ├── trace_1_clean_pass_2024-01.json / .md   # Committed Trace 1: Clean Pass (2024-01)
│   ├── trace_2_drift_and_repair_2025-01.json/.md# Committed Trace 2: Drift Caught & Handled + Repair (2025-01)
│   ├── trace_3_refusal_2025-99.json / .md      # Committed Trace 3: Refusal Path (2025-99)
│   ├── trace_4_durable_resume_2025-01.json/.md # Bonus Trace 4: Mid-Run Kill & Durable Resume
│   └── trace_5_retry_exhaustion_refusal.json/.md# Bonus Trace 5: Refusal via Retry Budget Exhaustion
└── tests/
    └── test_squad.py                           # 7 end-to-end integration tests
```

## Summary of Committed Runs

| Run Scenario | Target Month | Drift Findings | Builder Attempts | Verifier Outcome | Gross Passenger Revenue | Net Total Amount | Trace File |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | :--- |
| **1. Clean Pass** | `2024-01` | `0` (`CLEAN`) | `1` (`0` retries) | **ACCEPTED** (`8/8` checks) | `$2,354,271.15` | `$2,350,669.00` | `traces/trace_1_clean_pass_2024-01.md` |
| **2. Drift + Repair** | `2025-01` | `5` (`ADAPTABLE_DRIFT`) | `2` (`1` retry) | **REJECT** Attempt 1 (`V4`) -> **ACCEPT** Attempt 2 (`8/8`) | `$2,689,193.04` | `$2,684,635.71` | `traces/trace_2_drift_and_repair_2025-01.md` |
| **3. Refusal (Source Corruption)** | `2025-99` | `4` (`FATAL_REFUSAL`) | `0` (Halted at gate) | **REFUSED** (`DRIFT_FATAL_UNMAPPED_PAYMENT_TYPES`) | *Purged / None* | *Purged / None* | `traces/trace_3_refusal_2025-99.md` |
| **4. Durable Kill & Resume** | `2025-01` | `5` (`ADAPTABLE_DRIFT`) | `2` (Resumed after kill) | **ACCEPTED** (`skipped=['STAGE_1_PROFILE_AND_DRIFT', 'STAGE_2_BUILD_ATTEMPT_1']`) | `$2,689,193.04` | `$2,684,635.71` | `traces/trace_4_durable_resume_2025-01.md` |
| **5. Refusal (Retry Exhaustion)** | `2025-01` (`max_retries=1`) | `5` (`ADAPTABLE_DRIFT`) | `1` (Budget exhausted) | **REFUSED** (`REFUSE_MAX_RETRIES_EXHAUSTED`) | *Purged / None* | *Purged / None* | `traces/trace_5_retry_exhaustion_refusal.md` |
