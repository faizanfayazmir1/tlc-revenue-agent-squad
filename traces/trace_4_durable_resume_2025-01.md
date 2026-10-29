# Execution Trace — `test_durable_kill_resume_2025_01` (Month: `2025-01`)

| Attribute | Value |
| :--- | :--- |
| **Final Status** | `ACCEPTED` |
| **Source Parquet** | `C:\Users\faiza\Downloads\spyne\spyne-revenue-squad\spyne-revenue-squad\data\yellow_tripdata_2025-01.parquet` |
| **Steps Used / Max** | `5 / 12` |
| **Repair Retries Used / Max** | `1 / 3` |
| **Cost Used / Max Budget** | `$0.0500 / $0.25` |
| **Resumed From Checkpoint** | `True` (Skipped stages: `['STAGE_1_PROFILE_AND_DRIFT', 'STAGE_2_BUILD_ATTEMPT_1']`) |
| **Final Output CSV** | `C:\Users\faiza\Downloads\spyne\spyne-revenue-squad\spyne-revenue-squad\outputs\revenue_summary_2025-01.csv` |
| **Verifier Evidence / Refusal** | `C:\Users\faiza\Downloads\spyne\spyne-revenue-squad\spyne-revenue-squad\outputs\verifier_evidence_2025-01.json` |

## 1. Drift Detection & Source Assessment
- **Overall Severity**: `ADAPTABLE_DRIFT`
- **Decision**: `ADAPT_AND_PROCEED`
- **Rationale**: Detected 5 adaptable schema/domain drift finding(s) in 2025-01 (columns=['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee']). Safe adaptation plan synthesized.
- **Active Surcharge Columns**: `['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee']`

| Drift ID | Category | Severity | Field | Baseline vs. Observed | Action |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `DRIFT_CASE_AIRPORT_FEE` | `SCHEMA_CASE_CHANGE` | `ADAPTABLE_DRIFT` | `airport_fee` | **Expected**: Column 'Airport_fee' (exact case in 2024 schema)<br>**Observed**: Column renamed with different casing: 'airport_fee' | Map canonical 'Airport_fee' -> 'airport_fee' in SQL Builder and Verifier. |
| `DRIFT_NEW_FEE_COL_CBD_CONGESTION_FEE` | `SCHEMA_NEW_REVENUE_COLUMN` | `ADAPTABLE_DRIFT` | `cbd_congestion_fee` | **Expected**: 19-column 2024 schema (no MTA CBD congestion fee column)<br>**Observed**: 20-column 2025 schema includes 'cbd_congestion_fee' (non-null in 100.0% of rows, range [-0.75, 0.75]) | Adapt SQL Builder and Verifier to incorporate COALESCE("cbd_congestion_fee", 0.0) into `surcharges_and_fees_usd` and accounting identity checks. |
| `DRIFT_DOMAIN_PAYMENT_TYPE_0` | `DOMAIN_NEW_ENUM_VALUE` | `ADAPTABLE_DRIFT` | `payment_type` | **Expected**: payment_type in {1..6} (2024 baseline codes)<br>**Observed**: payment_type=0 ('Flex Fare trip') present in 3,298 rows (6.00% of dataset) | Expand payment_type dimension to map 0 -> 'Flex Fare trip' and enforce COALESCE(..., 0.0) across all nullable surcharge columns. |
| `DRIFT_NULLABLE_FEE_CONGESTION_SURCHARGE` | `DATA_NULLABILITY_SHIFT` | `ADAPTABLE_DRIFT` | `congestion_surcharge` | **Expected**: 0% NULLs in fee column 'congestion_surcharge'<br>**Observed**: 3,298 rows (6.00%) have NULL in 'congestion_surcharge' | Wrap 'congestion_surcharge' in COALESCE("congestion_surcharge", 0.0) in all SQL expressions. |
| `DRIFT_NULLABLE_FEE_AIRPORT_FEE` | `DATA_NULLABILITY_SHIFT` | `ADAPTABLE_DRIFT` | `airport_fee` | **Expected**: 0% NULLs in fee column 'airport_fee'<br>**Observed**: 3,298 rows (6.00%) have NULL in 'airport_fee' | Wrap 'airport_fee' in COALESCE("airport_fee", 0.0) in all SQL expressions. |

## 2. SQL Builder & Repair Loop Attempts
### Attempt 1
- **Reasoning**: Attempt 1 for 2025-01: Joined yellow_tripdata with taxi_zone_lookup on PULocationID=LocationID using LEFT JOIN (fallback LocationID 264). Included surcharge columns ['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'] (row_level_coalesce=False, strict_temporal_filter=True). Segregated negative reversal trips (110 rows) from valid billable trips (54861 rows) to produce gross_passenger_revenue_usd=$2,689,193.04.
- **Applied Fixes**: `["Adapted schema mapping for 5 drift finding(s): active_surcharge_columns=['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'], payment_types=[0, 1, 2, 3, 4, 5, 6]."]`
- **Candidate Gross Passenger Revenue**: `$2,689,193.04` (205 rows, 1018.55 ms)

### Attempt 2
- **Reasoning**: Attempt 2 for 2025-01: Joined yellow_tripdata with taxi_zone_lookup on PULocationID=LocationID using LEFT JOIN (fallback LocationID 264). Included surcharge columns ['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'] (row_level_coalesce=True, strict_temporal_filter=True). Segregated negative reversal trips (110 rows) from valid billable trips (54861 rows) to produce gross_passenger_revenue_usd=$2,689,193.04.
- **Applied Fixes**: `['Wrapped all surcharge/fee columns in row-level COALESCE(col, 0.0) to prevent SQL NULL propagation on 2025 Flex Fare trips.', "Adapted schema mapping for 5 drift finding(s): active_surcharge_columns=['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'], payment_types=[0, 1, 2, 3, 4, 5, 6]."]`
- **Candidate Gross Passenger Revenue**: `$2,689,193.04` (205 rows, 1071.93 ms)

## 3. Independent Verifier Audit Reports
### Verification of Attempt 1 — Decision: `REJECT`
- **Builder Isolation Enforced**: `True` (Verifier received only candidate CSV + contract, zero builder reasoning/SQL)
- **Rejection Codes**: `['REJECT_V4_ACCOUNTING_IDENTITY_MISMATCH']`

| Check ID | Check Name | Passed | Expected Contract | Observed Evidence |
| :--- | :--- | :--- | :--- | :--- |
| `V1_SCHEMA_AND_COMPLETENESS` | Output Schema Completeness & Non-Null Integrity | **PASS** | All 18 contractual columns present, row_count > 0, and 0 NULL/NaN cells. | missing_cols=[], row_count=205, null_or_empty_cells=0 |
| `V2_TEMPORAL_BOUNDARY_AUDIT` | Strict Target Month Trip Count Reconciliation | **PASS** | Summary total_trip_count must equal exact in-month trip count (54,971) within [2025-01-01 00:00:00, 2025-02-01 00:00:00), excluding 29 out-of-month spillover rows. | summary_total_trip_count=54,971, independent_in_month_rows=54,971, excluded_spillover_rows=29 |
| `V3_GLOBAL_REVENUE_RECONCILIATION` | Global Recognized Revenue & Itemized Source Reconciliation | **PASS** | Summary gross_passenger_revenue_usd must match independent SQL query ($2,689,193.04) and raw itemized sum ($2,689,193.04) within rounding tolerance. | summary_gross=$2,689,193.04, independent_gross=$2,689,193.04 (diff=$0.00), independent_itemized_sum=$2,689,193.04 (raw_itemized_diff=$0.00) |
| `V4_ACCOUNTING_IDENTITY_CHECK` | Summary Cell Horizontal Accounting Identity (Components == Gross Revenue) | **FAIL** | For every (pickup_zone, payment_type) row: |fare + surcharges_and_fees + tip + tolls - gross_passenger_revenue_usd| <= $0.05 | 41 summary cells failed horizontal accounting identity. First failure: Zone=138 (LaGuardia Airport) / PT=0 (Flex Fare trip): components_sum=$13,746.25 vs gross=$14,188.00 (diff=$441.75) |
| `V5_ZONE_JOIN_INTEGRITY` | TLC Pickup Zone Referential Integrity (1..265) | **PASS** | All pu_location_id values map to official TLC LocationIDs [1..265] with zero unjoined zones. | invalid_or_unjoined_zone_rows=0 |
| `V6_PAYMENT_TYPE_DOMAIN_CHECK` | Authorized Payment Type Code & Label Contract | **PASS** | All payment_type_id values in authorized set [0, 1, 2, 3, 4, 5, 6] with valid finance labels. | All 205 rows use authorized payment types. |
| `V7_CELL_SPOT_CHECK` | Independent 5-Cell Granular SQL Spot-Check Audit | **PASS** | Top 5 (pickup_zone, payment_type) cells match independent raw SQL queries to the cent. | 5/5 audited cells matched exact trip counts and gross_passenger_revenue_usd. |
| `V8_STATISTICAL_SANITY_BOUNDS` | Unit Economics & Non-Negative Revenue Sanity Check | **PASS** | gross_passenger_revenue_usd >= 0 and avg_revenue_per_trip_usd within [$2.50, $500.00] for cells with >= 10 trips. | 0 unit-economics outliers across 205 groups. |

### Verification of Attempt 2 — Decision: `ACCEPT`
- **Builder Isolation Enforced**: `True` (Verifier received only candidate CSV + contract, zero builder reasoning/SQL)
- **Rejection Codes**: `NONE`

| Check ID | Check Name | Passed | Expected Contract | Observed Evidence |
| :--- | :--- | :--- | :--- | :--- |
| `V1_SCHEMA_AND_COMPLETENESS` | Output Schema Completeness & Non-Null Integrity | **PASS** | All 18 contractual columns present, row_count > 0, and 0 NULL/NaN cells. | missing_cols=[], row_count=205, null_or_empty_cells=0 |
| `V2_TEMPORAL_BOUNDARY_AUDIT` | Strict Target Month Trip Count Reconciliation | **PASS** | Summary total_trip_count must equal exact in-month trip count (54,971) within [2025-01-01 00:00:00, 2025-02-01 00:00:00), excluding 29 out-of-month spillover rows. | summary_total_trip_count=54,971, independent_in_month_rows=54,971, excluded_spillover_rows=29 |
| `V3_GLOBAL_REVENUE_RECONCILIATION` | Global Recognized Revenue & Itemized Source Reconciliation | **PASS** | Summary gross_passenger_revenue_usd must match independent SQL query ($2,689,193.04) and raw itemized sum ($2,689,193.04) within rounding tolerance. | summary_gross=$2,689,193.04, independent_gross=$2,689,193.04 (diff=$0.00), independent_itemized_sum=$2,689,193.04 (raw_itemized_diff=$0.00) |
| `V4_ACCOUNTING_IDENTITY_CHECK` | Summary Cell Horizontal Accounting Identity (Components == Gross Revenue) | **PASS** | For every (pickup_zone, payment_type) row: |fare + surcharges_and_fees + tip + tolls - gross_passenger_revenue_usd| <= $0.05 | 0 accounting identity failures across 205 summary rows. |
| `V5_ZONE_JOIN_INTEGRITY` | TLC Pickup Zone Referential Integrity (1..265) | **PASS** | All pu_location_id values map to official TLC LocationIDs [1..265] with zero unjoined zones. | invalid_or_unjoined_zone_rows=0 |
| `V6_PAYMENT_TYPE_DOMAIN_CHECK` | Authorized Payment Type Code & Label Contract | **PASS** | All payment_type_id values in authorized set [0, 1, 2, 3, 4, 5, 6] with valid finance labels. | All 205 rows use authorized payment types. |
| `V7_CELL_SPOT_CHECK` | Independent 5-Cell Granular SQL Spot-Check Audit | **PASS** | Top 5 (pickup_zone, payment_type) cells match independent raw SQL queries to the cent. | 5/5 audited cells matched exact trip counts and gross_passenger_revenue_usd. |
| `V8_STATISTICAL_SANITY_BOUNDS` | Unit Economics & Non-Negative Revenue Sanity Check | **PASS** | gross_passenger_revenue_usd >= 0 and avg_revenue_per_trip_usd within [$2.50, $500.00] for cells with >= 10 trips. | 0 unit-economics outliers across 205 groups. |

## 5. Chronological Event Log

| Seq | Timestamp (UTC) | Agent | Stage | Action | Tool | Status | Summary |
| :---: | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `2026-10-29T02:23:07Z` | `RevenueSquadOrchestrator` | `STAGE_0_INIT` | `INITIALIZE_OR_RESUME_RUN` | `DurableStateStore.init_or_load_run` | `STARTED` | Started new run 'test_durable_kill_resume_2025_01' for month=2025-01 (bounds: max_steps=12, max_retries=3, max_cost_usd=$0.25). |
| 2 | `2026-10-29T02:23:08Z` | `SourceProfilerAndDriftAgent` | `STAGE_1_PROFILE_AND_DRIFT` | `PROFILE_SOURCE_AND_DETECT_DRIFT` | `inspect_parquet_schema + execute_readonly_sql` | `ADAPT_AND_PROCEED` | Detected 5 adaptable schema/domain drift finding(s) in 2025-01 (columns=['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee']). Safe adaptation plan synthesized. |
| 3 | `2026-10-29T02:23:09Z` | `SQLRevenueBuilderAgent` | `STAGE_2_BUILD_ATTEMPT_1` | `BUILD_REVENUE_SUMMARY_SQL` | `execute_readonly_sql` | `BUILT_CANDIDATE` | Attempt 1 for 2025-01: Joined yellow_tripdata with taxi_zone_lookup on PULocationID=LocationID using LEFT JOIN (fallback LocationID 264). Included surcharge columns ['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'] (row_level_coalesce=False, strict_temporal_filter=True). Segregated negative reversal trips (110 rows) from valid billable trips (54861 rows) to produce gross_passenger_revenue_usd=$2,689,193.04. |
| 4 | `2026-10-29T02:23:09Z` | `RevenueSquadOrchestrator` | `STAGE_2_BUILD_ATTEMPT_1` | `SIMULATE_PROCESS_KILL` | `-` | `KILLED` | Process intentionally killed after STAGE_2_BUILD_ATTEMPT_1 to test durable state resumption. |
| 5 | `2026-10-29T02:23:09Z` | `RevenueSquadOrchestrator` | `STAGE_0_INIT` | `INITIALIZE_OR_RESUME_RUN` | `DurableStateStore.init_or_load_run` | `RESUMED` | Resumed existing durable run 'test_durable_kill_resume_2025_01' for month=2025-01 (bounds: max_steps=12, max_retries=3, max_cost_usd=$0.25). |
| 6 | `2026-10-29T02:23:09Z` | `RevenueSquadOrchestrator` | `STAGE_1_PROFILE_AND_DRIFT` | `SKIP_COMPLETED_STAGE` | `DurableStateStore.get_checkpoint` | `SKIPPED_FROM_DURABLE_STATE` | Loaded STAGE_1_PROFILE_AND_DRIFT from durable SQLite checkpoint without re-profiling (55,000 rows, decision=ADAPT_AND_PROCEED). |
| 7 | `2026-10-29T02:23:09Z` | `RevenueSquadOrchestrator` | `STAGE_2_BUILD_ATTEMPT_1` | `SKIP_COMPLETED_STAGE` | `DurableStateStore.get_checkpoint` | `SKIPPED_FROM_DURABLE_STATE` | Loaded STAGE_2_BUILD_ATTEMPT_1 from durable SQLite checkpoint (gross_passenger_revenue_usd=$2,689,193.04). |
| 8 | `2026-10-29T02:23:09Z` | `IndependentVerifierAgent` | `STAGE_3_VERIFY_ATTEMPT_1` | `INDEPENDENT_CONTRACT_VERIFICATION` | `execute_readonly_sql (independent audit queries V1..V8)` | `REJECT` | Verifier REJECTED Attempt 1 for 2025-01: 7/8 checks passed. Rejection codes: ['REJECT_V4_ACCOUNTING_IDENTITY_MISMATCH']. |
| 9 | `2026-10-29T02:23:10Z` | `SQLRevenueBuilderAgent` | `STAGE_2_BUILD_ATTEMPT_2` | `BUILD_REVENUE_SUMMARY_SQL` | `execute_readonly_sql` | `BUILT_CANDIDATE` | Attempt 2 for 2025-01: Joined yellow_tripdata with taxi_zone_lookup on PULocationID=LocationID using LEFT JOIN (fallback LocationID 264). Included surcharge columns ['extra', 'mta_tax', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'] (row_level_coalesce=True, strict_temporal_filter=True). Segregated negative reversal trips (110 rows) from valid billable trips (54861 rows) to produce gross_passenger_revenue_usd=$2,689,193.04. |
| 10 | `2026-10-29T02:23:11Z` | `IndependentVerifierAgent` | `STAGE_3_VERIFY_ATTEMPT_2` | `INDEPENDENT_CONTRACT_VERIFICATION` | `execute_readonly_sql (independent audit queries V1..V8)` | `ACCEPT` | Verifier ACCEPTED Attempt 2 for 2025-01: 8/8 checks passed. Rejection codes: None. |
| 11 | `2026-10-29T02:23:11Z` | `RevenueSquadOrchestrator` | `STAGE_4_FINALIZE_ACCEPTED` | `PUBLISH_VERIFIED_REVENUE_SUMMARY` | `-` | `ACCEPTED` | Run 'test_durable_kill_resume_2025_01' ACCEPTED on Attempt 2. Verified gross_passenger_revenue_usd=$2,689,193.04 across 205 (pickup_zone, payment_type) groups. |
