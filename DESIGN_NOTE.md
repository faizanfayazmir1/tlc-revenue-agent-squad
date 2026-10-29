# Design Note: Resilient Multi-Agent Squad for NYC TLC Monthly Revenue Reporting

**Assignment**: Spyne Member of Technical Staff Take-Home Assignment  
**Data Source**: [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (`2024-01` vs. `2025-01` + Unseen Months)  
**Target Output**: Monthly Passenger Revenue Summary by Pickup Zone (`PULocationID`) and Payment Type (`payment_type`)

## 1. Revenue Definition & Financial Defense

Under the [NYC TLC Yellow Taxi Data Dictionary (March 18, 2025)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf), `total_amount` represents the total amount charged to passengers (excluding cash tips, which taximeters do not record). However, raw monthly TLC Parquet files contain three well-documented operational artifacts that corrupt naive `SUM(total_amount)` aggregations:

1. **Vendor Reversal & Dispute Records (`total_amount <= 0` or `fare_amount < 0`)**: TPEP vendors (notably Curb Mobility, `VendorID=2`) append negative offsetting rows when a trip is disputed (`payment_type=4`) or voided (`payment_type=6`). Naive summation produces negative revenue cells for `Dispute` categories and distorts per-trip unit economics.
2. **Taximeter Clock-Drift Spillover**: Every monthly Parquet file contains stray timestamps from outside the target calendar month (e.g., `2023-12-31` or `2024-12-31` late-night pickups flushed in January batches).
3. **2025 Regulatory & Domain Changes**: Starting January 5, 2025, NYC introduced the MTA Congestion Relief Zone charge (`cbd_congestion_fee = $0.75/trip`), lowercase `airport_fee`, and `payment_type = 0` (`Flex Fare trip`), where `congestion_surcharge` and `airport_fee` are `NULL` while `cbd_congestion_fee` and `total_amount` are populated.

**Defended Contract (`contracts/revenue_contract.json`)**:
- **Primary Recognized Revenue (`gross_passenger_revenue_usd`)**: `ROUND(SUM(total_amount), 2)` restricted to **in-month valid billable trips** (`tpep_pickup_datetime >= YYYY-MM-01 00:00:00 AND tpep_pickup_datetime < next_month-01 00:00:00 AND total_amount > 0 AND fare_amount >= 0`).
- **Horizontal Itemized Reconciliation**: Every summary cell reports `fare_revenue_usd`, `surcharges_and_fees_usd` (`COALESCE(extra,0) + COALESCE(mta_tax,0) + COALESCE(improvement_surcharge,0) + COALESCE(congestion_surcharge,0) + COALESCE(airport_fee,0) [+ COALESCE(cbd_congestion_fee,0)]`), `tip_revenue_usd`, and `tolls_revenue_usd`, which must sum strictly to `gross_passenger_revenue_usd` within `$0.05`.
- **Segregated Adjustments (`reversal_adjustments_usd` & `net_total_amount_usd`)**: Negative/zero adjustment rows are reported separately in `reversal_adjustments_usd` (`reversal_trip_count`) and netted in `net_total_amount_usd = gross_passenger_revenue_usd + reversal_adjustments_usd`. Finance receives both gross recognized billings and net settlement totals without negative distortions hiding inside gross revenue.

**Explicit One-Line Assumptions (Where the Brief Is Silent)**:
- *Assumption 1*: Pickup timestamps (`tpep_pickup_datetime`), rather than dropoff timestamps, govern monthly revenue recognition cutoffs (accrual at meter engagement).
- *Assumption 2*: Unmapped or `NULL` `PULocationID` values are preserved via `LEFT JOIN` to `taxi_zone_lookup.csv` and bucketed into official TLC LocationID `264 (Unknown)` so zero dollars of revenue are silently dropped by inner joins.

## 2. Agent Boundaries: Why the Split Earns Its Complexity (30%)

A single monolithic LLM agent that profiles schemas, writes SQL, and self-checks its own output suffers from **confirmation bias**: if its prompt or SQL logic forgets a `WHERE` filter or drops a nullable column via `col1 + col2`, its self-verification query repeats the exact same mistake. Our architecture separates responsibilities across **three specialized agents and a deterministic orchestrator**, each operating with distinct tools and asymmetric permissions:

| Component | Module | Tools & Permissions | State Visibility & Responsibility |
| :--- | :--- | :--- | :--- |
| **Orchestrator** | `squad/orchestrator.py` | `DurableStateStore` (SQLite ACID checkpointing) | Enforces `max_steps=12`, `max_retries=3`, `max_cost_usd=$0.25`; routes outputs across the Verifier firewall; purges unverified files on refusal. |
| **Agent 1: Source Profiler & Drift Agent** | `squad/profiler_agent.py` | `inspect_parquet_schema`, read-only profiling SQL | Compares raw Parquet physical schema, null ratios, domain values, temporal bounds, and row-level accounting against `schema_baseline_2024.json`. Emits `DriftAssessment` (`CLEAN`, `ADAPTABLE_DRIFT`, or `FATAL_REFUSAL`). |
| **Agent 2: SQL Builder & Repair Agent** | `squad/builder_agent.py` | `execute_readonly_sql`, CSV/JSON writer | Synthesizes aggregation SQL using `AdaptationPlan`. On repair iterations (`attempt > 1`), receives **only** the Verifier's `rejection_codes` and `remediation_hints` to fix SQL defects. |
| **Agent 3: Independent Verifier Agent** | `squad/verifier_agent.py` | `VerifierInput` firewall, 8 independent SQL audits (`V1`–`V8`) | Sees **only** the candidate CSV/JSON output, `revenue_contract.json`, and raw read-only tables. **Never** sees Builder reasoning, Builder SQL, or orchestrator history. |

## 3. Verifier Independence & The Repair Loop (25%)

Verifier independence is enforced at **both the type level and runtime boundary** in `squad/models.py` (`VerifierInput`) and `squad/verifier_agent.py`:
1. **Hard Input Firewall (`VerifierIsolationError`)**: `VerifierInput` sets `extra = "forbid"` and `IndependentVerifierAgent.verify()` explicitly scans incoming keys against `FORBIDDEN_BUILDER_KEYS = {"builder_reasoning", "sql_query", "builder_sql", "drift_assessment", "applied_fixes", "agent_messages", "thought_process"}`. Any attempt to leak the Builder's reasoning or SQL raises `VerifierIsolationError` (verified by `test_3_verifier_architectural_isolation_firewall`).
2. **Eight Independent Raw-Data SQL Audits (`V1`–`V8`)**: Instead of reviewing the Builder's SQL string, the Verifier writes its own independent audit queries against `yellow_tripdata` and `taxi_zone_lookup`:
   - `V1_SCHEMA_AND_COMPLETENESS`: Verifies all 18 contractual columns exist with zero `NULL`/`NaN` cells.
   - `V2_TEMPORAL_BOUNDARY_AUDIT`: Independently queries raw `yellow_tripdata` to verify `SUM(total_trip_count)` matches exact in-month rows (`[YYYY-MM-01, next_month-01)`), catching clock-drift spillover leaks.
   - `V3_GLOBAL_REVENUE_RECONCILIATION`: Independently sums raw eligible revenue and itemized components to verify global gross and net totals within rounding bounds.
   - `V4_ACCOUNTING_IDENTITY_CHECK`: Audits every row of the candidate summary to ensure `|fare + surcharges_and_fees + tip + tolls - gross_passenger_revenue_usd| <= $0.05`.
   - `V5_ZONE_JOIN_INTEGRITY` & `V6_PAYMENT_TYPE_DOMAIN_CHECK`: Verifies all zones map to `1..265` and all payment types belong to `{0..6}`.
   - `V7_CELL_SPOT_CHECK` & `V8_STATISTICAL_SANITY_BOUNDS`: Independently re-queries the top 5 `(PULocationID, payment_type)` cells to verify cent-level accuracy and checks unit-economics bounds (`$2.50 <= avg_rev_per_trip <= $500.00`).
3. **Live Repair Loop on `2025-01`**: In `2025-01`, `payment_type = 0` (`Flex Fare trip`) rows have `NULL` in `congestion_surcharge` and `airport_fee`. On **Attempt 1**, the Builder adapts the new column names (`cbd_congestion_fee`, `airport_fee`) but sums surcharge columns without row-level `COALESCE`, causing SQL `NULL` propagation to zero out `surcharges_and_fees_usd` on Flex Fare rows (`components_sum = $10,947.20` vs. `gross = $12,018.45` in Midtown Center). The Verifier catches this via `V4_ACCOUNTING_IDENTITY_CHECK` (`REJECT_V4_ACCOUNTING_IDENTITY_MISMATCH`), rejects Attempt 1, and returns a structured remediation hint. On **Attempt 2**, the Builder wraps every fee column in `COALESCE(col, 0.0)`, and the Verifier accepts Attempt 2 (`8/8` checks passed).

## 4. How Drift Is Handled vs. Escalated to a Human (25% Drift / 20% Judgement)

`SourceProfilerAndDriftAgent` classifies every observed schema or data change into a strict 3-tier decision matrix:

| Tier | Trigger Conditions Observed in Profiler | Squad Action | Concrete Example in Committed Traces |
| :--- | :--- | :--- | :--- |
| **Tier 1: Clean (`CLEAN`)** | 19 baseline columns match `schema_baseline_2024.json`; payment types in `{1..6}`; `<5%` accounting mismatch. | `PROCEED_CLEAN` | **`trace_1_clean_pass_2024-01`**: 50,000 raw rows (49,982 in-month, 18 spillover excluded); accepted on Attempt 1 (`$2,354,271.15` gross). |
| **Tier 2: Adaptable Drift (`ADAPTABLE_DRIFT`)** | Case changes (`Airport_fee` -> `airport_fee`), known regulatory fee additions (`cbd_congestion_fee`), extended enum (`payment_type=0` Flex Fare), nullable fee columns. | `ADAPT_AND_PROCEED` | **`trace_2_drift_and_repair_2025-01`**: 5 drift findings caught; Attempt 1 rejected by Verifier `V4`; Attempt 2 repaired and accepted (`$2,689,193.04` gross). |
| **Tier 3: Fatal Refusal (`FATAL_REFUSAL`)** | Missing required grouping/revenue column; unknown monetary column; `>0.5%` unmapped `payment_type`; `>5%` rows failing itemized accounting reconciliation; `<80%` in-month timestamps; or `max_retries=3` exhausted. | `STOP_AND_REFUSE` (Purge outputs & escalate) | **`trace_3_refusal_2025-99`**: Caught `14.08%` unmapped `payment_type=99` and `38.01%` rows with `$25.00` `total_amount` corruption. Halted in 1 step (`$0.0080`) and emitted `refusal_report_2025-99.json`. |

## 5. Honesty About Where It Breaks

1. **Label-Swap Semantic Inversion**: If an upstream vendor bug swaps `payment_type = 1` (Credit Card) and `payment_type = 2` (Cash) labels while keeping `tip_amount = 0` or adjusting `total_amount` so horizontal accounting (`V4`) still holds, schema and accounting checks will pass. Mitigating this requires historical cross-month distribution drift checks (e.g., flagging if Credit Card share drops from ~75% to <20%).
2. **Sub-Threshold Corruption (`<0.05` per row or `<5%` of rows)**: Because real TLC data has minor meter rounding discrepancies on ~0.1–0.5% of trips, `revenue_contract.json` sets a `$0.05` row tolerance and `5%` row-count ceiling for Stage 1 fast refusal (while `V3` enforces global reconciliation). A subtle bug that underbills every ride by `$0.01` while simultaneously reducing `total_amount` by `$0.01` cannot be caught without external bank-settlement reconciliation.
3. **SQLite In-Memory Scaling Ceiling**: When `duckdb` is not installed, `SQLDataEngine` loads the monthly Parquet into an in-memory SQLite database via PyArrow/Pandas. This handles 50k–3.5M rows on a 16GB machine in seconds, but a 50M-row multi-month file on a memory-constrained container would OOM without streaming DuckDB execution.
