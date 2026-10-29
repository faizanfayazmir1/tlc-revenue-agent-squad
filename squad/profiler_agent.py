"""
Agent 1: Source Profiler & Drift Detection Agent (`SourceProfilerAndDriftAgent`).
Uses real SQL and Parquet inspection tools (`inspect_parquet_schema` and
`execute_readonly_sql`) to profile the incoming monthly Parquet file, compare it
against the 2024 baseline contract (`contracts/schema_baseline_2024.json`) and
finance quality contract (`contracts/revenue_contract.json`), and decide whether
to proceed cleanly, adapt to structural/domain drift, or stop and refuse.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

from squad.models import (
    AdaptationPlan,
    ColumnProfile,
    DriftAssessment,
    DriftFinding,
    DriftSeverity,
    TableProfile,
)
from squad.sql_engine import SQLDataEngine, compute_month_bounds


class SourceProfilerAndDriftAgent:
    """
    Profiles the raw Parquet dataset via SQL and Arrow metadata, detects schema
    and semantic drift, and determines whether the squad can safely adapt or must refuse.
    """

    AGENT_NAME = "SourceProfilerAndDriftAgent"

    def __init__(self, baseline_contract_path: Path, revenue_contract_path: Path) -> None:
        self.baseline = json.loads(Path(baseline_contract_path).read_text(encoding="utf-8"))
        self.revenue_contract = json.loads(Path(revenue_contract_path).read_text(encoding="utf-8"))

    def profile_and_detect_drift(
        self,
        month: str,
        engine: SQLDataEngine,
    ) -> Tuple[TableProfile, DriftAssessment]:
        arrow_schema = engine.inspect_parquet_schema()
        month_start, month_end = compute_month_bounds(month)

        # Total row count
        total_rows_res = engine.execute_readonly_sql("SELECT COUNT(*) AS cnt FROM yellow_tripdata")
        total_rows = int(total_rows_res[0]["cnt"]) if total_rows_res else 0

        # Build case-insensitive map of actual columns
        actual_cols_by_lower: Dict[str, str] = {c.lower(): c for c in arrow_schema.keys()}

        # Profile each column via SQL
        col_profiles: Dict[str, ColumnProfile] = {}
        for col_name, type_info in arrow_schema.items():
            stats = engine.execute_readonly_sql(
                f'''
                SELECT
                    SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) AS null_cnt,
                    MIN("{col_name}") AS min_val,
                    MAX("{col_name}") AS max_val
                FROM yellow_tripdata
                '''
            )[0]
            null_cnt = int(stats["null_cnt"] or 0)
            col_profiles[col_name] = ColumnProfile(
                name=col_name,
                arrow_type=type_info["arrow_type"],
                logical_type=type_info["logical_type"],
                null_count=null_cnt,
                null_ratio=round(null_cnt / max(total_rows, 1), 6),
                min_value=stats["min_val"],
                max_value=stats["max_val"],
            )

        # Resolve core column names (supporting case drift)
        pickup_ts_col = actual_cols_by_lower.get("tpep_pickup_datetime")
        payment_col = actual_cols_by_lower.get("payment_type")
        total_col = actual_cols_by_lower.get("total_amount")
        fare_col = actual_cols_by_lower.get("fare_amount")

        # Temporal profile
        if pickup_ts_col and total_rows > 0:
            temp_res = engine.execute_readonly_sql(
                f'''
                SELECT
                    SUM(CASE WHEN "{pickup_ts_col}" >= ? AND "{pickup_ts_col}" < ? THEN 1 ELSE 0 END) AS in_month,
                    MIN("{pickup_ts_col}") AS min_ts,
                    MAX("{pickup_ts_col}") AS max_ts
                FROM yellow_tripdata
                ''',
                (month_start, month_end),
            )[0]
            in_month_rows = int(temp_res["in_month"] or 0)
            min_pickup_ts = str(temp_res["min_ts"]) if temp_res["min_ts"] is not None else None
            max_pickup_ts = str(temp_res["max_ts"]) if temp_res["max_ts"] is not None else None
        else:
            in_month_rows = 0
            min_pickup_ts = None
            max_pickup_ts = None

        out_of_month_rows = total_rows - in_month_rows
        in_month_ratio = round(in_month_rows / max(total_rows, 1), 6)

        # Payment type distribution
        payment_counts: Dict[str, int] = {}
        unmapped_pt_rows = 0
        extended_pt_map = {
            int(k): v
            for k, v in self.baseline["adaptable_known_extensions"]["extended_payment_types"].items()
        }
        if payment_col and total_rows > 0:
            pt_rows = engine.execute_readonly_sql(
                f'''
                SELECT "{payment_col}" AS pt, COUNT(*) AS cnt
                FROM yellow_tripdata
                GROUP BY "{payment_col}"
                ORDER BY pt
                '''
            )
            for r in pt_rows:
                pt_val = r["pt"]
                cnt = int(r["cnt"])
                key_str = "NULL" if pt_val is None else str(int(pt_val))
                payment_counts[key_str] = cnt
                if pt_val is None or int(pt_val) not in extended_pt_map:
                    unmapped_pt_rows += cnt

        unmapped_pt_ratio = round(unmapped_pt_rows / max(total_rows, 1), 6)

        # Detect all surcharge/fee columns present in the file
        base_surcharge_Logical = [
            "extra",
            "mta_tax",
            "improvement_surcharge",
            "congestion_surcharge",
            "airport_fee",
        ]
        detected_fee_cols: List[str] = []
        for s_col in base_surcharge_Logical:
            if s_col in actual_cols_by_lower:
                detected_fee_cols.append(actual_cols_by_lower[s_col])
        for ext_col in self.baseline["adaptable_known_extensions"]["additional_fee_columns"]:
            if ext_col.lower() in actual_cols_by_lower:
                detected_fee_cols.append(actual_cols_by_lower[ext_col.lower()])
        # Also catch any unknown '*_fee' or '*_surcharge' column
        for c_name in arrow_schema.keys():
            c_low = c_name.lower()
            if (c_low.endswith("_fee") or c_low.endswith("_surcharge")) and c_name not in detected_fee_cols:
                detected_fee_cols.append(c_name)

        # Accounting reconciliation check on raw rows
        neg_or_zero_rows = 0
        mismatch_rows = 0
        max_discrepancy = 0.0
        if total_col and fare_col and total_rows > 0:
            comp_terms: List[str] = []
            for base_c in ("fare_amount", "tip_amount", "tolls_amount"):
                if base_c in actual_cols_by_lower:
                    comp_terms.append(f'COALESCE("{actual_cols_by_lower[base_c]}", 0.0)')
            for fee_c in detected_fee_cols:
                comp_terms.append(f'COALESCE("{fee_c}", 0.0)')
            comp_expr = " + ".join(comp_terms) if comp_terms else "0.0"
            tol = float(self.revenue_contract["quality_thresholds"]["row_accounting_tolerance_usd"])

            acc_res = engine.execute_readonly_sql(
                f'''
                SELECT
                    SUM(CASE WHEN "{total_col}" <= 0 OR "{fare_col}" < 0 THEN 1 ELSE 0 END) AS neg_rows,
                    SUM(CASE WHEN ABS(COALESCE("{total_col}", 0.0) - ({comp_expr})) > ? THEN 1 ELSE 0 END) AS mismatch_cnt,
                    MAX(ABS(COALESCE("{total_col}", 0.0) - ({comp_expr}))) AS max_diff
                FROM yellow_tripdata
                ''',
                (tol,),
            )[0]
            neg_or_zero_rows = int(acc_res["neg_rows"] or 0)
            mismatch_rows = int(acc_res["mismatch_cnt"] or 0)
            max_discrepancy = round(float(acc_res["max_diff"] or 0.0), 4)

        mismatch_ratio = round(mismatch_rows / max(total_rows, 1), 6)

        profile = TableProfile(
            month=month,
            parquet_path=str(engine.parquet_path),
            total_rows=total_rows,
            in_month_rows=in_month_rows,
            out_of_month_rows=out_of_month_rows,
            in_month_ratio=in_month_ratio,
            min_pickup_ts=min_pickup_ts,
            max_pickup_ts=max_pickup_ts,
            columns=col_profiles,
            payment_type_counts=payment_counts,
            unmapped_payment_type_rows=unmapped_pt_rows,
            unmapped_payment_type_ratio=unmapped_pt_ratio,
            negative_or_zero_total_rows=neg_or_zero_rows,
            accounting_mismatch_rows=mismatch_rows,
            accounting_mismatch_ratio=mismatch_ratio,
            max_accounting_discrepancy_usd=max_discrepancy,
            detected_fee_columns=detected_fee_cols,
        )

        assessment = self._evaluate_drift(month, profile, arrow_schema)
        return profile, assessment

    def _evaluate_drift(
        self,
        month: str,
        profile: TableProfile,
        arrow_schema: Dict[str, Dict[str, str]],
    ) -> DriftAssessment:
        findings: List[DriftFinding] = []
        expected_cols: Dict[str, Dict[str, object]] = self.baseline["expected_columns"]
        actual_by_lower = {c.lower(): c for c in arrow_schema.keys()}
        column_mapping: Dict[str, str] = {}

        # 1. Check expected baseline columns (missing or case drift)
        for base_col, spec in expected_cols.items():
            if base_col in arrow_schema:
                column_mapping[base_col] = base_col
            elif base_col.lower() in actual_by_lower:
                actual_name = actual_by_lower[base_col.lower()]
                column_mapping[base_col] = actual_name
                findings.append(
                    DriftFinding(
                        drift_id=f"DRIFT_CASE_{base_col.upper()}",
                        category="SCHEMA_CASE_CHANGE",
                        severity=DriftSeverity.ADAPTABLE_DRIFT,
                        column_or_field=actual_name,
                        baseline_expectation=f"Column '{base_col}' (exact case in 2024 schema)",
                        observed_reality=f"Column renamed with different casing: '{actual_name}'",
                        impact_on_revenue="Case-sensitive SQL references or Parquet readers would fail or drop airport fees.",
                        proposed_action=f"Map canonical '{base_col}' -> '{actual_name}' in SQL Builder and Verifier.",
                    )
                )
            else:
                is_req = bool(spec.get("required", False))
                sev = DriftSeverity.FATAL_REFUSAL if is_req else DriftSeverity.ADAPTABLE_DRIFT
                findings.append(
                    DriftFinding(
                        drift_id=f"DRIFT_MISSING_{base_col.upper()}",
                        category="SCHEMA_MISSING_COLUMN",
                        severity=sev,
                        column_or_field=base_col,
                        baseline_expectation=f"Required={is_req} column '{base_col}' present",
                        observed_reality=f"Column '{base_col}' is completely absent from Parquet schema",
                        impact_on_revenue=(
                            "Cannot compute or verify contractual revenue breakdown without required column."
                            if is_req
                            else "Non-revenue metadata column missing; safe to ignore."
                        ),
                        proposed_action="Refuse execution and escalate." if is_req else "Omit metadata column.",
                    )
                )

        # 2. Check extra/new columns not in 2024 baseline (e.g., 2025 `cbd_congestion_fee`)
        baseline_lower_set = {c.lower() for c in expected_cols.keys()}
        known_ext_fees = {
            c.lower() for c in self.baseline["adaptable_known_extensions"]["additional_fee_columns"]
        }
        for actual_col in arrow_schema.keys():
            if actual_col.lower() not in baseline_lower_set:
                if actual_col.lower() in known_ext_fees:
                    column_mapping[actual_col] = actual_col
                    findings.append(
                        DriftFinding(
                            drift_id=f"DRIFT_NEW_FEE_COL_{actual_col.upper()}",
                            category="SCHEMA_NEW_REVENUE_COLUMN",
                            severity=DriftSeverity.ADAPTABLE_DRIFT,
                            column_or_field=actual_col,
                            baseline_expectation="19-column 2024 schema (no MTA CBD congestion fee column)",
                            observed_reality=(
                                f"20-column 2025 schema includes '{actual_col}' "
                                f"(non-null in {100.0 * (1.0 - profile.columns[actual_col].null_ratio):.1f}% of rows, "
                                f"range [{profile.columns[actual_col].min_value}, {profile.columns[actual_col].max_value}])"
                            ),
                            impact_on_revenue=(
                                f"'{actual_col}' is included in passenger `total_amount` starting Jan 5, 2025. "
                                "Omitting it from `surcharges_and_fees_usd` and itemized reconciliation causes "
                                "component sum to understate `total_amount` by $0.75/trip on CBD rides."
                            ),
                            proposed_action=(
                                f"Adapt SQL Builder and Verifier to incorporate COALESCE(\"{actual_col}\", 0.0) "
                                "into `surcharges_and_fees_usd` and accounting identity checks."
                            ),
                        )
                    )
                else:
                    # Unknown extra column: if it looks like a fee/revenue column, refuse rather than guess!
                    is_monetary = any(
                        tok in actual_col.lower() for tok in ("fee", "surcharge", "tax", "amount", "fare", "toll")
                    )
                    findings.append(
                        DriftFinding(
                            drift_id=f"DRIFT_UNRECOGNIZED_COL_{actual_col.upper()}",
                            category="SCHEMA_UNKNOWN_COLUMN",
                            severity=DriftSeverity.FATAL_REFUSAL if is_monetary else DriftSeverity.ADAPTABLE_DRIFT,
                            column_or_field=actual_col,
                            baseline_expectation="Only recognized 2024/2025 TLC columns",
                            observed_reality=f"Unrecognized column '{actual_col}' found in source file",
                            impact_on_revenue=(
                                "Unrecognized monetary column may alter revenue accounting semantics."
                                if is_monetary
                                else "Non-monetary auxiliary column does not affect revenue."
                            ),
                            proposed_action=(
                                "Refuse and escalate to Finance to define accounting treatment."
                                if is_monetary
                                else "Ignore auxiliary column."
                            ),
                        )
                    )

        # 3. Check payment_type domain drift (e.g., 2025 `payment_type = 0` Flex Fare or invalid `99`)
        baseline_pts = {int(k): v for k, v in self.baseline["baseline_payment_types"].items()}
        extended_pts = {
            int(k): v
            for k, v in self.baseline["adaptable_known_extensions"]["extended_payment_types"].items()
        }
        for pt_str, cnt in profile.payment_type_counts.items():
            if pt_str == "NULL":
                continue
            pt_id = int(pt_str)
            if pt_id not in baseline_pts and pt_id in extended_pts:
                findings.append(
                    DriftFinding(
                        drift_id=f"DRIFT_DOMAIN_PAYMENT_TYPE_{pt_id}",
                        category="DOMAIN_NEW_ENUM_VALUE",
                        severity=DriftSeverity.ADAPTABLE_DRIFT,
                        column_or_field="payment_type",
                        baseline_expectation="payment_type in {1..6} (2024 baseline codes)",
                        observed_reality=(
                            f"payment_type={pt_id} ('{extended_pts[pt_id]}') present in {cnt:,} rows "
                            f"({100.0 * cnt / max(profile.total_rows, 1):.2f}% of dataset)"
                        ),
                        impact_on_revenue=(
                            "2025 Flex Fare trips have NULL in `congestion_surcharge` and `airport_fee` "
                            "while populating `cbd_congestion_fee` and `total_amount`. Un-coalesced addition "
                            "turns `surcharges_and_fees_usd` into NULL."
                        ),
                        proposed_action=(
                            f"Expand payment_type dimension to map 0 -> '{extended_pts[pt_id]}' and enforce "
                            "COALESCE(..., 0.0) across all nullable surcharge columns."
                        ),
                    )
                )

        max_unmapped_pt = float(self.revenue_contract["quality_thresholds"]["max_unmapped_payment_type_ratio"])
        if profile.unmapped_payment_type_ratio > max_unmapped_pt:
            findings.append(
                DriftFinding(
                    drift_id="DRIFT_FATAL_UNMAPPED_PAYMENT_TYPES",
                    category="DOMAIN_CONTRACT_VIOLATION",
                    severity=DriftSeverity.FATAL_REFUSAL,
                    column_or_field="payment_type",
                    baseline_expectation=f"Unmapped payment_type ratio <= {100.0 * max_unmapped_pt:.2f}%",
                    observed_reality=(
                        f"{profile.unmapped_payment_type_rows:,} rows ({100.0 * profile.unmapped_payment_type_ratio:.2f}%) "
                        f"have unauthorized payment_type codes: {profile.payment_type_counts}"
                    ),
                    impact_on_revenue="Cannot attribute revenue to valid finance payment categories.",
                    proposed_action="Refuse to ship summary and escalate unmapped payment_type codes to Finance/Data Eng.",
                )
            )

        # 4. Check nullable fee columns (2025 Flex Fare trips introduce NULLs in congestion_surcharge / airport_fee)
        requires_null_coalesce = False
        for fee_col in profile.detected_fee_columns:
            col_prof = profile.columns.get(fee_col)
            if col_prof and col_prof.null_count > 0:
                requires_null_coalesce = True
                findings.append(
                    DriftFinding(
                        drift_id=f"DRIFT_NULLABLE_FEE_{fee_col.upper()}",
                        category="DATA_NULLABILITY_SHIFT",
                        severity=DriftSeverity.ADAPTABLE_DRIFT,
                        column_or_field=fee_col,
                        baseline_expectation=f"0% NULLs in fee column '{fee_col}'",
                        observed_reality=f"{col_prof.null_count:,} rows ({100.0 * col_prof.null_ratio:.2f}%) have NULL in '{fee_col}'",
                        impact_on_revenue="Raw `col1 + col2` SQL arithmetic yields NULL if any fee column is NULL.",
                        proposed_action=f"Wrap '{fee_col}' in COALESCE(\"{fee_col}\", 0.0) in all SQL expressions.",
                    )
                )

        # 5. Check temporal month alignment
        min_in_month_ratio = float(self.revenue_contract["quality_thresholds"]["min_in_month_trip_ratio"])
        if profile.in_month_ratio < min_in_month_ratio:
            findings.append(
                DriftFinding(
                    drift_id="DRIFT_FATAL_TEMPORAL_MISMATCH",
                    category="TEMPORAL_CORRUPTION",
                    severity=DriftSeverity.FATAL_REFUSAL,
                    column_or_field="tpep_pickup_datetime",
                    baseline_expectation=f">= {100.0 * min_in_month_ratio:.1f}% of rows within target month {month}",
                    observed_reality=(
                        f"Only {profile.in_month_rows:,} / {profile.total_rows:,} rows "
                        f"({100.0 * profile.in_month_ratio:.2f}%) fall inside {month} "
                        f"(range [{profile.min_pickup_ts}, {profile.max_pickup_ts}])"
                    ),
                    impact_on_revenue="Source file does not match requested target calendar month.",
                    proposed_action="Refuse execution immediately; source file is mislabeled or temporally corrupt.",
                )
            )

        # 6. Check accounting reconciliation (total_amount vs sum of itemized components)
        max_acc_mismatch = float(self.revenue_contract["quality_thresholds"]["max_accounting_mismatch_row_ratio"])
        if profile.accounting_mismatch_ratio > max_acc_mismatch:
            findings.append(
                DriftFinding(
                    drift_id="DRIFT_FATAL_ACCOUNTING_RECONCILIATION_FAILURE",
                    category="ACCOUNTING_INTEGRITY_CORRUPTION",
                    severity=DriftSeverity.FATAL_REFUSAL,
                    column_or_field="total_amount",
                    baseline_expectation=(
                        f"Itemized components reconcile with total_amount within "
                        f"${self.revenue_contract['quality_thresholds']['row_accounting_tolerance_usd']:.2f} "
                        f"on >= {100.0 * (1.0 - max_acc_mismatch):.1f}% of rows"
                    ),
                    observed_reality=(
                        f"{profile.accounting_mismatch_rows:,} / {profile.total_rows:,} rows "
                        f"({100.0 * profile.accounting_mismatch_ratio:.2f}%) fail itemized accounting reconciliation "
                        f"(max discrepancy = ${profile.max_accounting_discrepancy_usd:.2f})"
                    ),
                    impact_on_revenue=(
                        "total_amount diverges fundamentally from the sum of fare + surcharges + tips + tolls. "
                        "Shipping either number would report unverified, corrupted revenue to Finance."
                    ),
                    proposed_action="Refuse to publish summary and escalate accounting discrepancy to Finance & TLC Vendor Ops.",
                )
            )

        # Determine overall severity
        if any(f.severity == DriftSeverity.FATAL_REFUSAL for f in findings):
            overall = DriftSeverity.FATAL_REFUSAL
            decision = "STOP_AND_REFUSE"
            can_proceed = False
            rationale = (
                f"Detected {sum(1 for f in findings if f.severity == DriftSeverity.FATAL_REFUSAL)} fatal "
                f"contract/integrity violation(s) in {month}. Refusing to compute or ship a potentially wrong revenue number."
            )
        elif any(f.severity == DriftSeverity.ADAPTABLE_DRIFT for f in findings):
            overall = DriftSeverity.ADAPTABLE_DRIFT
            decision = "ADAPT_AND_PROCEED"
            can_proceed = True
            rationale = (
                f"Detected {len(findings)} adaptable schema/domain drift finding(s) in {month} "
                f"(columns={profile.detected_fee_columns}). Safe adaptation plan synthesized."
            )
        else:
            overall = DriftSeverity.CLEAN
            decision = "PROCEED_CLEAN"
            can_proceed = True
            rationale = f"Source schema and domain distributions for {month} match 2024 baseline contract cleanly."

        plan = AdaptationPlan(
            can_proceed=can_proceed,
            column_mapping=column_mapping,
            active_surcharge_columns=profile.detected_fee_columns,
            payment_type_mapping=extended_pts if overall == DriftSeverity.ADAPTABLE_DRIFT else baseline_pts,
            requires_null_coalesce_on_fees=requires_null_coalesce,
            requires_strict_temporal_filter=(profile.out_of_month_rows > 0),
            notes=[f.proposed_action for f in findings],
        )

        return DriftAssessment(
            month=month,
            overall_severity=overall,
            decision=decision,
            findings=findings,
            adaptation_plan=plan,
            summary_rationale=rationale,
        )
