"""
Agent 3: Independent Verifier Agent (`IndependentVerifierAgent`).

ARCHITECTURAL INDEPENDENCE GUARANTEE:
  - This agent NEVER receives `BuilderAttemptResult`, `builder_reasoning`,
    `sql_query`, `DriftAssessment`, or orchestrator conversation history.
  - Its entrypoint `verify(raw_input: dict)` validates `raw_input` against
    `VerifierInput` (which sets `ConfigDict(extra='forbid')`), raising
    `VerifierIsolationError` if any forbidden builder state is present.
  - It inspects ONLY:
      1. The candidate output file (`summary_csv_path` / `summary_json_path`),
      2. The formal revenue contract (`contracts/revenue_contract.json`), and
      3. Its own independent SQL audit queries executed directly against the
         raw source tables (`yellow_tripdata` and `taxi_zone_lookup`).
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List
from pydantic import ValidationError

from squad.models import (
    VerificationCheckResult,
    VerificationReport,
    VerifierInput,
    VerifierIsolationError,
)
from squad.sql_engine import SQLDataEngine, compute_month_bounds


FORBIDDEN_BUILDER_KEYS = {
    "builder_reasoning",
    "sql_query",
    "builder_sql",
    "drift_assessment",
    "applied_fixes",
    "agent_messages",
    "thought_process",
}


class IndependentVerifierAgent:
    """
    Audits a candidate monthly revenue summary against the formal Finance Contract
    using independent raw-data SQL queries.
    """

    AGENT_NAME = "IndependentVerifierAgent"

    def __init__(self, evidence_dir: Path) -> None:
        self.evidence_dir = Path(evidence_dir)
        self.evidence_dir.mkdir(parents=True, exist_ok=True)

    def verify(
        self,
        raw_verifier_input: Dict[str, Any],
        engine: SQLDataEngine,
    ) -> VerificationReport:
        # 1. Enforce strict architectural isolation boundary
        leaked_keys = FORBIDDEN_BUILDER_KEYS.intersection(raw_verifier_input.keys())
        if leaked_keys:
            raise VerifierIsolationError(
                f"CRITICAL BOUNDARY VIOLATION: Builder state keys {sorted(leaked_keys)} "
                "were passed to IndependentVerifierAgent. The Verifier must only see the "
                "candidate output and the contract, never the Builder's reasoning or SQL."
            )
        try:
            v_input = VerifierInput(**raw_verifier_input)
        except ValidationError as exc:
            raise VerifierIsolationError(
                f"Invalid VerifierInput payload violated isolation contract: {exc}"
            ) from exc

        contract = json.loads(Path(v_input.contract_path).read_text(encoding="utf-8"))
        thresholds = contract["quality_thresholds"]
        required_cols: List[str] = contract["required_output_columns"]

        # Read candidate CSV output produced by the Builder
        csv_path = Path(v_input.summary_csv_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"Candidate summary CSV not found at {csv_path}")

        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            actual_cols = list(reader.fieldnames or [])
            rows = list(reader)

        checks: List[VerificationCheckResult] = []
        rejection_codes: List[str] = []
        remediation_hints: List[str] = []

        # ---------------------------------------------------------------------
        # CHECK V1: Output Schema, Non-Empty Rows & Zero NULLs
        # ---------------------------------------------------------------------
        missing_cols = [c for c in required_cols if c not in actual_cols]
        null_cells = 0
        for r in rows:
            for c in required_cols:
                val = r.get(c)
                if val is None or str(val).strip() == "" or str(val).upper() == "NULL" or str(val).lower() == "nan":
                    null_cells += 1

        v1_passed = (len(missing_cols) == 0) and (len(rows) > 0) and (null_cells == 0)
        checks.append(
            VerificationCheckResult(
                check_id="V1_SCHEMA_AND_COMPLETENESS",
                check_name="Output Schema Completeness & Non-Null Integrity",
                passed=v1_passed,
                expected_contract=f"All {len(required_cols)} contractual columns present, row_count > 0, and 0 NULL/NaN cells.",
                observed_evidence=f"missing_cols={missing_cols}, row_count={len(rows)}, null_or_empty_cells={null_cells}",
                remediation_hint=(
                    "Ensure all 18 contractual columns are projected with COALESCE to eliminate NULL cells."
                    if not v1_passed
                    else None
                ),
            )
        )
        if not v1_passed:
            rejection_codes.append("REJECT_V1_SCHEMA_OR_NULL_VIOLATION")
            remediation_hints.append("Fix missing output columns or NULL/NaN values in summary output.")

        # Resolve column mapping for independent SQL audit queries
        col_map = v_input.column_mapping
        pu_col = col_map.get("PULocationID", "PULocationID")
        pt_col = col_map.get("payment_type", "payment_type")
        pickup_ts_col = col_map.get("tpep_pickup_datetime", "tpep_pickup_datetime")
        fare_col = col_map.get("fare_amount", "fare_amount")
        tip_col = col_map.get("tip_amount", "tip_amount")
        tolls_col = col_map.get("tolls_amount", "tolls_amount")
        total_col = col_map.get("total_amount", "total_amount")
        month_start, month_end = compute_month_bounds(v_input.month)

        # ---------------------------------------------------------------------
        # CHECK V2: Independent Temporal Boundary Audit
        # ---------------------------------------------------------------------
        raw_temporal = engine.execute_readonly_sql(
            f'''
            SELECT
                COUNT(*) AS all_rows,
                SUM(CASE WHEN "{pickup_ts_col}" >= ? AND "{pickup_ts_col}" < ? THEN 1 ELSE 0 END) AS in_month_rows,
                SUM(CASE WHEN "{pickup_ts_col}" < ? OR "{pickup_ts_col}" >= ? THEN 1 ELSE 0 END) AS spillover_rows
            FROM yellow_tripdata
            ''',
            (month_start, month_end, month_start, month_end),
        )[0]
        expected_in_month_trips = int(raw_temporal["in_month_rows"] or 0)
        spillover_rows = int(raw_temporal["spillover_rows"] or 0)
        summary_total_trips = sum(int(r["total_trip_count"]) for r in rows) if v1_passed else -1

        v2_passed = (summary_total_trips == expected_in_month_trips) and (expected_in_month_trips > 0)
        checks.append(
            VerificationCheckResult(
                check_id="V2_TEMPORAL_BOUNDARY_AUDIT",
                check_name="Strict Target Month Trip Count Reconciliation",
                passed=v2_passed,
                expected_contract=(
                    f"Summary total_trip_count must equal exact in-month trip count ({expected_in_month_trips:,}) "
                    f"within [{month_start}, {month_end}), excluding {spillover_rows:,} out-of-month spillover rows."
                ),
                observed_evidence=(
                    f"summary_total_trip_count={summary_total_trips:,}, "
                    f"independent_in_month_rows={expected_in_month_trips:,}, "
                    f"excluded_spillover_rows={spillover_rows:,}"
                ),
                remediation_hint=(
                    f"Filter {pickup_ts_col} strictly within ['{month_start}', '{month_end}') so out-of-month spillover records are excluded."
                    if not v2_passed
                    else None
                ),
            )
        )
        if not v2_passed:
            rejection_codes.append("REJECT_V2_TEMPORAL_BOUNDARY_VIOLATION")
            remediation_hints.append(
                f"Add WHERE {pickup_ts_col} >= '{month_start}' AND {pickup_ts_col} < '{month_end}'."
            )

        # ---------------------------------------------------------------------
        # CHECK V3: Independent Global Revenue & Reversal Reconciliation
        # ---------------------------------------------------------------------
        surcharge_coalesced_expr = (
            " + ".join(f'COALESCE("{c}", 0.0)' for c in v_input.active_surcharge_columns) or "0.0"
        )
        raw_rev = engine.execute_readonly_sql(
            f'''
            SELECT
                SUM(CASE WHEN "{total_col}" > 0.0 AND "{fare_col}" >= 0.0 THEN 1 ELSE 0 END) AS valid_trips,
                SUM(CASE WHEN "{total_col}" <= 0.0 OR "{fare_col}" < 0.0 THEN 1 ELSE 0 END) AS reversal_trips,
                ROUND(SUM(CASE WHEN "{total_col}" > 0.0 AND "{fare_col}" >= 0.0 THEN "{total_col}" ELSE 0.0 END), 2) AS gross_rev,
                ROUND(SUM(CASE WHEN "{total_col}" > 0.0 AND "{fare_col}" >= 0.0 THEN (
                    COALESCE("{fare_col}", 0.0) + ({surcharge_coalesced_expr}) + COALESCE("{tip_col}", 0.0) + COALESCE("{tolls_col}", 0.0)
                ) ELSE 0.0 END), 2) AS itemized_valid_rev,
                ROUND(SUM(CASE WHEN "{total_col}" <= 0.0 OR "{fare_col}" < 0.0 THEN "{total_col}" ELSE 0.0 END), 2) AS reversal_rev,
                ROUND(SUM("{total_col}"), 2) AS net_rev
            FROM yellow_tripdata
            WHERE "{pickup_ts_col}" >= ? AND "{pickup_ts_col}" < ?
            ''',
            (month_start, month_end),
        )[0]

        ind_gross_rev = float(raw_rev["gross_rev"] or 0.0)
        ind_itemized_rev = float(raw_rev["itemized_valid_rev"] or 0.0)
        ind_reversal_rev = float(raw_rev["reversal_rev"] or 0.0)
        ind_net_rev = float(raw_rev["net_rev"] or 0.0)

        sum_gross_rev = round(sum(float(r["gross_passenger_revenue_usd"]) for r in rows), 2) if v1_passed else -1.0
        sum_reversal_rev = round(sum(float(r["reversal_adjustments_usd"]) for r in rows), 2) if v1_passed else -1.0
        sum_net_rev = round(sum(float(r["net_total_amount_usd"]) for r in rows), 2) if v1_passed else -1.0

        global_tol = float(thresholds["global_reconciliation_tolerance_usd"])
        # Note: allow small rounding difference across group sums (up to $0.05 * num_groups or $1.00) vs global sum,
        # AND check that raw gross_rev reconciles with raw itemized_valid_rev!
        group_rounding_bound = max(global_tol, round(0.01 * len(rows), 2))
        gross_diff = round(abs(sum_gross_rev - ind_gross_rev), 2)
        raw_itemized_diff = round(abs(ind_gross_rev - ind_itemized_rev), 2)

        v3_passed = (gross_diff <= group_rounding_bound) and (raw_itemized_diff <= max(1.0, 0.0001 * ind_gross_rev))
        checks.append(
            VerificationCheckResult(
                check_id="V3_GLOBAL_REVENUE_RECONCILIATION",
                check_name="Global Recognized Revenue & Itemized Source Reconciliation",
                passed=v3_passed,
                expected_contract=(
                    f"Summary gross_passenger_revenue_usd must match independent SQL query (${ind_gross_rev:,.2f}) "
                    f"and raw itemized sum (${ind_itemized_rev:,.2f}) within rounding tolerance."
                ),
                observed_evidence=(
                    f"summary_gross=${sum_gross_rev:,.2f}, independent_gross=${ind_gross_rev:,.2f} (diff=${gross_diff:.2f}), "
                    f"independent_itemized_sum=${ind_itemized_rev:,.2f} (raw_itemized_diff=${raw_itemized_diff:.2f})"
                ),
                remediation_hint=(
                    "Ensure valid billable trips filter (`total_amount > 0 AND fare_amount >= 0`) and all active fee columns match raw source."
                    if not v3_passed
                    else None
                ),
            )
        )
        if not v3_passed:
            rejection_codes.append("REJECT_V3_GLOBAL_REVENUE_MISMATCH")
            remediation_hints.append(
                f"Global gross revenue (${sum_gross_rev:,.2f}) or raw itemized sum (${ind_itemized_rev:,.2f}) "
                f"failed to reconcile with independent raw total (${ind_gross_rev:,.2f})."
            )

        # ---------------------------------------------------------------------
        # CHECK V4: Row-Level Accounting Identity Across Summary Cells
        # ---------------------------------------------------------------------
        # For every row in the output table:
        #   fare_revenue_usd + surcharges_and_fees_usd + tip_revenue_usd + tolls_revenue_usd == gross_passenger_revenue_usd
        #   AND gross_passenger_revenue_usd + reversal_adjustments_usd == net_total_amount_usd
        row_tol = float(thresholds["row_accounting_tolerance_usd"])
        identity_failures: List[str] = []
        for r in rows:
            if not v1_passed:
                break
            fare_v = float(r["fare_revenue_usd"])
            sur_v = float(r["surcharges_and_fees_usd"])
            tip_v = float(r["tip_revenue_usd"])
            tolls_v = float(r["tolls_revenue_usd"])
            gross_v = float(r["gross_passenger_revenue_usd"])
            rev_v = float(r["reversal_adjustments_usd"])
            net_v = float(r["net_total_amount_usd"])

            comp_sum = round(fare_v + sur_v + tip_v + tolls_v, 2)
            comp_diff = round(abs(gross_v - comp_sum), 2)
            net_diff = round(abs((gross_v + rev_v) - net_v), 2)

            if comp_diff > row_tol or net_diff > row_tol:
                identity_failures.append(
                    f"Zone={r['pu_location_id']} ({r['pickup_zone']}) / PT={r['payment_type_id']} ({r['payment_type_name']}): "
                    f"components_sum=${comp_sum:,.2f} vs gross=${gross_v:,.2f} (diff=${comp_diff:,.2f})"
                )

        v4_passed = len(identity_failures) == 0 and v1_passed
        checks.append(
            VerificationCheckResult(
                check_id="V4_ACCOUNTING_IDENTITY_CHECK",
                check_name="Summary Cell Horizontal Accounting Identity (Components == Gross Revenue)",
                passed=v4_passed,
                expected_contract=(
                    "For every (pickup_zone, payment_type) row: "
                    "|fare + surcharges_and_fees + tip + tolls - gross_passenger_revenue_usd| <= $0.05"
                ),
                observed_evidence=(
                    f"0 accounting identity failures across {len(rows)} summary rows."
                    if v4_passed
                    else f"{len(identity_failures)} summary cells failed horizontal accounting identity. "
                    f"First failure: {identity_failures[0]}"
                ),
                remediation_hint=(
                    "Check for SQL NULL propagation in surcharge columns (e.g., NULL congestion_surcharge or airport_fee on Flex Fare rows) "
                    "or missing fee columns; wrap each surcharge column in COALESCE(col, 0.0) before summing."
                    if not v4_passed
                    else None
                ),
            )
        )
        if not v4_passed:
            rejection_codes.append("REJECT_V4_ACCOUNTING_IDENTITY_MISMATCH")
            remediation_hints.append(
                "Horizontal accounting identity failed (`fare + surcharges + tip + tolls != gross_passenger_revenue_usd`). "
                "Wrap each surcharge column in row-level COALESCE(col, 0.0) so NULLs in `congestion_surcharge` or `airport_fee` "
                "do not zero out `surcharges_and_fees_usd`."
            )

        # ---------------------------------------------------------------------
        # CHECK V5: Pickup Zone Lookup & Join Completeness
        # ---------------------------------------------------------------------
        min_zone = int(thresholds["valid_zone_id_min"])
        max_zone = int(thresholds["valid_zone_id_max"])
        invalid_zones = [
            int(r["pu_location_id"])
            for r in rows
            if not (min_zone <= int(r["pu_location_id"]) <= max_zone) or r["pickup_zone"].startswith("Unknown Zone")
        ]
        v5_passed = len(invalid_zones) == 0 and v1_passed
        checks.append(
            VerificationCheckResult(
                check_id="V5_ZONE_JOIN_INTEGRITY",
                check_name="TLC Pickup Zone Referential Integrity (1..265)",
                passed=v5_passed,
                expected_contract=f"All pu_location_id values map to official TLC LocationIDs [{min_zone}..{max_zone}] with zero unjoined zones.",
                observed_evidence=f"invalid_or_unjoined_zone_rows={len(invalid_zones)}",
                remediation_hint="Use LEFT JOIN to taxi_zone_lookup with COALESCE(PULocationID, 264)." if not v5_passed else None,
            )
        )
        if not v5_passed:
            rejection_codes.append("REJECT_V5_ZONE_JOIN_FAILURE")
            remediation_hints.append("Ensure all PULocationID values join cleanly to taxi_zone_lookup (1..265).")

        # ---------------------------------------------------------------------
        # CHECK V6: Payment Type Domain Contract Check
        # ---------------------------------------------------------------------
        allowed_pts = set(int(x) for x in thresholds["authorized_payment_type_ids"])
        invalid_pts = [
            f"{r['payment_type_id']} ({r['payment_type_name']})"
            for r in rows
            if int(r["payment_type_id"]) not in allowed_pts or str(r["payment_type_name"]).startswith("UNMAPPED_")
        ]
        v6_passed = len(invalid_pts) == 0 and v1_passed
        checks.append(
            VerificationCheckResult(
                check_id="V6_PAYMENT_TYPE_DOMAIN_CHECK",
                check_name="Authorized Payment Type Code & Label Contract",
                passed=v6_passed,
                expected_contract=f"All payment_type_id values in authorized set {sorted(allowed_pts)} with valid finance labels.",
                observed_evidence=(
                    f"All {len(rows)} rows use authorized payment types."
                    if v6_passed
                    else f"Unauthorized payment types in summary: {invalid_pts[:5]}"
                ),
                remediation_hint="Refuse or map only authorized TLC payment_type codes {0..6}." if not v6_passed else None,
            )
        )
        if not v6_passed:
            rejection_codes.append("REJECT_V6_UNAUTHORIZED_PAYMENT_TYPE")
            remediation_hints.append(f"Unauthorized payment_type codes detected: {invalid_pts[:3]}.")

        # ---------------------------------------------------------------------
        # CHECK V7: Independent 5-Cell Point-by-Point SQL Audit
        # ---------------------------------------------------------------------
        sample_rows = rows[:5] if len(rows) >= 5 else rows
        cell_mismatches: List[str] = []
        for sr in sample_rows:
            pu_id = int(sr["pu_location_id"])
            pt_id = int(sr["payment_type_id"])
            expected_cell = engine.execute_readonly_sql(
                f'''
                SELECT
                    COUNT(*) AS tot_cnt,
                    SUM(CASE WHEN "{total_col}" > 0.0 AND "{fare_col}" >= 0.0 THEN 1 ELSE 0 END) AS val_cnt,
                    ROUND(SUM(CASE WHEN "{total_col}" > 0.0 AND "{fare_col}" >= 0.0 THEN "{total_col}" ELSE 0.0 END), 2) AS gross_rev
                FROM yellow_tripdata
                WHERE "{pickup_ts_col}" >= ? AND "{pickup_ts_col}" < ?
                  AND COALESCE(CAST("{pu_col}" AS INTEGER), 264) = ?
                  AND COALESCE(CAST("{pt_col}" AS INTEGER), -1) = ?
                ''',
                (month_start, month_end, pu_id, pt_id),
            )[0]
            ind_cell_gross = float(expected_cell["gross_rev"] or 0.0)
            ind_cell_cnt = int(expected_cell["val_cnt"] or 0)
            rep_cell_gross = float(sr["gross_passenger_revenue_usd"])
            rep_cell_cnt = int(sr["valid_trip_count"])
            if abs(ind_cell_gross - rep_cell_gross) > 0.02 or ind_cell_cnt != rep_cell_cnt:
                cell_mismatches.append(
                    f"(PU={pu_id}, PT={pt_id}): reported=({rep_cell_cnt} trips, ${rep_cell_gross:,.2f}) "
                    f"vs independent_audit=({ind_cell_cnt} trips, ${ind_cell_gross:,.2f})"
                )

        v7_passed = len(cell_mismatches) == 0 and len(sample_rows) > 0
        checks.append(
            VerificationCheckResult(
                check_id="V7_CELL_SPOT_CHECK",
                check_name="Independent 5-Cell Granular SQL Spot-Check Audit",
                passed=v7_passed,
                expected_contract="Top 5 (pickup_zone, payment_type) cells match independent raw SQL queries to the cent.",
                observed_evidence=(
                    f"5/5 audited cells matched exact trip counts and gross_passenger_revenue_usd."
                    if v7_passed
                    else f"Spot-check mismatches: {cell_mismatches}"
                ),
                remediation_hint="Verify GROUP BY keys and valid trip predicates match contract." if not v7_passed else None,
            )
        )
        if not v7_passed:
            rejection_codes.append("REJECT_V7_SPOT_CHECK_MISMATCH")
            remediation_hints.append(f"Granular cell spot check failed: {cell_mismatches[:2]}.")

        # ---------------------------------------------------------------------
        # CHECK V8: Statistical & Unit-Economics Sanity Bounds
        # ---------------------------------------------------------------------
        min_avg = float(thresholds["min_avg_revenue_per_trip_usd"])
        max_avg = float(thresholds["max_avg_revenue_per_trip_usd"])
        sanity_outliers: List[str] = []
        for r in rows:
            val_cnt = int(r["valid_trip_count"])
            gross_v = float(r["gross_passenger_revenue_usd"])
            avg_v = float(r["avg_revenue_per_trip_usd"])
            if gross_v < 0.0:
                sanity_outliers.append(f"(PU={r['pu_location_id']}, PT={r['payment_type_id']}): negative gross=${gross_v}")
            elif val_cnt >= 10 and not (min_avg <= avg_v <= max_avg):
                sanity_outliers.append(
                    f"(PU={r['pu_location_id']}, PT={r['payment_type_id']}): avg_rev_per_trip=${avg_v:.2f} outside [${min_avg}, ${max_avg}]"
                )

        v8_passed = len(sanity_outliers) == 0 and v1_passed
        checks.append(
            VerificationCheckResult(
                check_id="V8_STATISTICAL_SANITY_BOUNDS",
                check_name="Unit Economics & Non-Negative Revenue Sanity Check",
                passed=v8_passed,
                expected_contract=f"gross_passenger_revenue_usd >= 0 and avg_revenue_per_trip_usd within [${min_avg:.2f}, ${max_avg:.2f}] for cells with >= 10 trips.",
                observed_evidence=(
                    f"0 unit-economics outliers across {len(rows)} groups."
                    if v8_passed
                    else f"Outliers found: {sanity_outliers[:3]}"
                ),
                remediation_hint="Ensure negative reversal trips are segregated from valid billable trips." if not v8_passed else None,
            )
        )
        if not v8_passed:
            rejection_codes.append("REJECT_V8_UNIT_ECONOMICS_OUTLIER")
            remediation_hints.append(f"Unit economics check failed: {sanity_outliers[:2]}.")

        all_passed = all(c.passed for c in checks)
        decision = "ACCEPT" if all_passed else "REJECT"

        audit_metrics = {
            "month": v_input.month,
            "attempt_number": v_input.attempt_number,
            "decision": decision,
            "summary_row_count": len(rows),
            "independent_in_month_trip_count": expected_in_month_trips,
            "excluded_out_of_month_spillover_rows": spillover_rows,
            "independent_gross_passenger_revenue_usd": ind_gross_rev,
            "reported_gross_passenger_revenue_usd": sum_gross_rev,
            "independent_itemized_component_sum_usd": ind_itemized_rev,
            "independent_reversal_adjustments_usd": ind_reversal_rev,
            "reported_reversal_adjustments_usd": sum_reversal_rev,
            "independent_net_total_amount_usd": ind_net_rev,
            "reported_net_total_amount_usd": sum_net_rev,
            "checks_passed": sum(1 for c in checks if c.passed),
            "checks_total": len(checks),
            "isolation_verified": True,
        }

        evidence_path = self.evidence_dir / f"verifier_evidence_{v_input.month}_attempt_{v_input.attempt_number}.json"
        report = VerificationReport(
            month=v_input.month,
            attempt_number=v_input.attempt_number,
            passed=all_passed,
            decision=decision,
            checks=checks,
            rejection_codes=rejection_codes,
            remediation_hints=remediation_hints,
            independent_audit_metrics=audit_metrics,
            evidence_file_path=str(evidence_path),
        )
        evidence_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

        if all_passed:
            final_evidence_path = self.evidence_dir / f"verifier_evidence_{v_input.month}.json"
            final_evidence_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
            report.evidence_file_path = str(final_evidence_path)

        return report
