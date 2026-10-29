# Execution Trace — `run_2025-99` (Month: `2025-99`)

| Attribute | Value |
| :--- | :--- |
| **Final Status** | `REFUSED` |
| **Source Parquet** | `/working_dir/c_850c878e8a25c2ca/spyne-revenue-squad/data/yellow_tripdata_2025-99.parquet` |
| **Steps Used / Max** | `1 / 12` |
| **Repair Retries Used / Max** | `0 / 3` |
| **Cost Used / Max Budget** | `$0.0080 / $0.25` |
| **Resumed From Checkpoint** | `False` (Skipped stages: `[]`) |
| **Final Output CSV** | `NONE (Refused — unverified output purged)` |
| **Verifier Evidence / Refusal** | `/working_dir/c_850c878e8a25c2ca/spyne-revenue-squad/outputs/refusal_report_2025-99.json` |

## 1. Drift Detection & Source Assessment
- **Overall Severity**: `FATAL_REFUSAL`
- **Decision**: `STOP_AND_REFUSE`
- **Rationale**: Detected 2 fatal contract/integrity violation(s) in 2025-99. Refusing to compute or ship a potentially wrong revenue number.
- **Active Surcharge Columns**: `['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee']`

| Drift ID | Category | Severity | Field | Baseline vs. Observed | Action |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `DRIFT_CASE_AIRPORT_FEE` | `SCHEMA_CASE_CHANGE` | `ADAPTABLE_DRIFT` | `airport_fee` | **Expected**: Column 'Airport_fee' (exact case in 2024 schema)<br>**Observed**: Column renamed with different casing: 'airport_fee' | Map canonical 'Airport_fee' -> 'airport_fee' in SQL Builder and Verifier. |
| `DRIFT_NEW_FEE_COL_CBD_CONGESTION_FEE` | `SCHEMA_NEW_REVENUE_COLUMN` | `ADAPTABLE_DRIFT` | `cbd_congestion_fee` | **Expected**: 19-column 2024 schema (no MTA CBD congestion fee column)<br>**Observed**: 20-column 2025 schema includes 'cbd_congestion_fee' (non-null in 100.0% of rows, range [0.75, 0.75]) | Adapt SQL Builder and Verifier to incorporate COALESCE("cbd_congestion_fee", 0.0) into `surcharges_and_fees_usd` and accounting identity checks. |
| `DRIFT_FATAL_UNMAPPED_PAYMENT_TYPES` | `DOMAIN_CONTRACT_VIOLATION` | `FATAL_REFUSAL` | `payment_type` | **Expected**: Unmapped payment_type ratio <= 0.50%<br>**Observed**: 1,646 rows (13.72%) have unauthorized payment_type codes: {'1': 8183, '2': 2171, '99': 1646} | Refuse to ship summary and escalate unmapped payment_type codes to Finance/Data Eng. |
| `DRIFT_FATAL_ACCOUNTING_RECONCILIATION_FAILURE` | `ACCOUNTING_INTEGRITY_CORRUPTION` | `FATAL_REFUSAL` | `total_amount` | **Expected**: Itemized components reconcile with total_amount within $0.05 on >= 95.0% of rows<br>**Observed**: 4,593 / 12,000 rows (38.27%) fail itemized accounting reconciliation (max discrepancy = $25.00) | Refuse to publish summary and escalate accounting discrepancy to Finance & TLC Vendor Ops. |

## 4. Refusal & Human Escalation Report
- **Refusal Stage**: `STAGE_1_DRIFT_DETECTION_GATE`
- **Reason Code**: `DRIFT_FATAL_UNMAPPED_PAYMENT_TYPES`
- **Headline**: Refused to ship 2025-99 revenue summary due to fatal source data/contract violation.
- **Detailed Explanation**: Detected 2 fatal contract/integrity violation(s) in 2025-99. Refusing to compute or ship a potentially wrong revenue number.
- **Violated Contracts**: `['DRIFT_FATAL_UNMAPPED_PAYMENT_TYPES', 'DRIFT_FATAL_ACCOUNTING_RECONCILIATION_FAILURE']`
- **Human Escalation Action**: Escalate to Finance & TLC Vendor Data Engineering: do not ingest or report this monthly file until the corrupted `total_amount` accounting discrepancy and unmapped `payment_type` codes are fixed upstream.

## 5. Chronological Event Log

| Seq | Timestamp (UTC) | Agent | Stage | Action | Tool | Status | Summary |
| :---: | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `2026-10-29T01:15:51Z` | `RevenueSquadOrchestrator` | `STAGE_0_INIT` | `INITIALIZE_OR_RESUME_RUN` | `DurableStateStore.init_or_load_run` | `STARTED` | Started new run 'run_2025-99' for month=2025-99 (bounds: max_steps=12, max_retries=3, max_cost_usd=$0.25). |
| 2 | `2026-10-29T01:15:52Z` | `SourceProfilerAndDriftAgent` | `STAGE_1_PROFILE_AND_DRIFT` | `PROFILE_SOURCE_AND_DETECT_DRIFT` | `inspect_parquet_schema + execute_readonly_sql` | `STOP_AND_REFUSE` | Detected 2 fatal contract/integrity violation(s) in 2025-99. Refusing to compute or ship a potentially wrong revenue number. |
| 3 | `2026-10-29T01:15:52Z` | `RevenueSquadOrchestrator` | `STAGE_4_FINALIZE_REFUSAL` | `REFUSE_AND_ESCALATE_TO_HUMAN` | `-` | `REFUSED` | Refused to ship 2025-99 revenue summary due to fatal source data/contract violation. Reason=DRIFT_FATAL_UNMAPPED_PAYMENT_TYPES. Escalation=Escalate to Finance & TLC Vendor Data Engineering: do not ingest or report this monthly file until the corrupted `total_amount` accounting discrepancy and unmapped `payment_type` codes are fixed upstream. |
