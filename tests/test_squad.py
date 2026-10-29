#!/usr/bin/env python3
"""
End-to-end unit and integration test suite for the Spyne Monthly Revenue Agent Squad.
Tests:
  1. 2024-01 Clean Pass (0 retries, 8/8 verifier checks)
  2. 2025-01 Schema & Domain Drift + Verifier-Builder Repair Loop
  3. Verifier Architectural Isolation Firewall (`VerifierIsolationError` on leaked builder state)
  4. Durable State Mid-Run Process Kill & Exact Checkpoint Resumption
  5. Refusal Path on Source Accounting/Domain Corruption (2025-99)
  6. Refusal Path on Bounded Retry Exhaustion (purges candidate outputs & escalates)
  7. Read-Only SQL Security Guardrail (`PermissionError` on DDL/DML)
"""

from __future__ import annotations

import importlib.abc
import importlib.util
from pathlib import Path
import sys
import unittest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


class _DirectStatPathFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path=None, target=None):
        rel = Path(*fullname.split("."))
        pkg_init = ROOT_DIR / rel / "__init__.py"
        if pkg_init.is_file():
            return importlib.util.spec_from_file_location(
                fullname,
                str(pkg_init),
                submodule_search_locations=[str(pkg_init.parent)],
            )
        mod_file = (ROOT_DIR / rel).with_suffix(".py")
        if mod_file.is_file():
            return importlib.util.spec_from_file_location(fullname, str(mod_file))
        return None


sys.meta_path.insert(0, _DirectStatPathFinder())

from squad.models import DriftSeverity, RunStatus, VerifierIsolationError
from squad.orchestrator import RevenueSquadOrchestrator, SimulatedProcessKill
from squad.sql_engine import SQLDataEngine
from squad.verifier_agent import IndependentVerifierAgent


class TestSpyneRevenueSquad(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.data_dir = ROOT_DIR / "data"
        cls.zones_path = cls.data_dir / "taxi_zone_lookup.csv"
        cls.p_2024 = cls.data_dir / "yellow_tripdata_2024-01.parquet"
        cls.p_2025 = cls.data_dir / "yellow_tripdata_2025-01.parquet"
        cls.p_refusal = cls.data_dir / "yellow_tripdata_2025-99.parquet"

    def test_1_clean_pass_2024_01(self) -> None:
        orch = RevenueSquadOrchestrator(root_dir=ROOT_DIR)
        trace = orch.run(
            month="2024-01",
            parquet_path=self.p_2024,
            zone_lookup_path=self.zones_path,
            run_id="test_clean_2024_01",
            reset_state=True,
        )
        self.assertEqual(trace.status, RunStatus.ACCEPTED)
        self.assertEqual(trace.retries_used, 0)
        self.assertIsNotNone(trace.drift_assessment)
        self.assertEqual(trace.drift_assessment.overall_severity, DriftSeverity.CLEAN)
        self.assertEqual(len(trace.builder_attempts), 1)
        self.assertEqual(len(trace.verification_reports), 1)
        self.assertTrue(trace.verification_reports[0].passed)
        self.assertTrue(Path(trace.final_output_csv).exists())

    def test_2_drift_and_repair_loop_2025_01(self) -> None:
        orch = RevenueSquadOrchestrator(root_dir=ROOT_DIR)
        trace = orch.run(
            month="2025-01",
            parquet_path=self.p_2025,
            zone_lookup_path=self.zones_path,
            run_id="test_drift_2025_01",
            reset_state=True,
        )
        self.assertEqual(trace.status, RunStatus.ACCEPTED)
        self.assertIsNotNone(trace.drift_assessment)
        self.assertEqual(trace.drift_assessment.overall_severity, DriftSeverity.ADAPTABLE_DRIFT)
        drift_ids = {f.drift_id for f in trace.drift_assessment.findings}
        self.assertIn("DRIFT_NEW_FEE_COL_CBD_CONGESTION_FEE", drift_ids)
        self.assertIn("DRIFT_CASE_AIRPORT_FEE", drift_ids)
        self.assertIn("DRIFT_DOMAIN_PAYMENT_TYPE_0", drift_ids)

        # Verify Repair Loop: Attempt 1 rejected by Verifier, Attempt 2 accepted
        self.assertEqual(len(trace.builder_attempts), 2)
        self.assertEqual(len(trace.verification_reports), 2)
        self.assertFalse(trace.verification_reports[0].passed)
        self.assertIn(
            "REJECT_V4_ACCOUNTING_IDENTITY_MISMATCH",
            trace.verification_reports[0].rejection_codes,
        )
        self.assertTrue(trace.verification_reports[1].passed)
        self.assertEqual(trace.retries_used, 1)

    def test_3_verifier_architectural_isolation_firewall(self) -> None:
        verifier = IndependentVerifierAgent(ROOT_DIR / "outputs")
        engine = SQLDataEngine(self.p_2024, self.zones_path)
        try:
            leaked_payload = {
                "month": "2024-01",
                "attempt_number": 1,
                "summary_csv_path": str(ROOT_DIR / "outputs" / "revenue_summary_2024-01.csv"),
                "summary_json_path": str(ROOT_DIR / "outputs" / "revenue_summary_2024-01.json"),
                "contract_path": str(ROOT_DIR / "contracts" / "revenue_contract.json"),
                "parquet_path": str(self.p_2024),
                "zone_lookup_path": str(self.zones_path),
                "active_surcharge_columns": ["extra", "mta_tax", "improvement_surcharge", "congestion_surcharge", "Airport_fee"],
                "column_mapping": {"PULocationID": "PULocationID"},
                "builder_reasoning": "I think this query is right because I wrote it.",
            }
            with self.assertRaises(VerifierIsolationError):
                verifier.verify(leaked_payload, engine)
        finally:
            engine.close()

    def test_4_durable_state_kill_and_resume(self) -> None:
        orch = RevenueSquadOrchestrator(root_dir=ROOT_DIR)
        run_id = "test_durable_kill_resume_2025_01"

        # Step A: Kill process mid-run right after STAGE_2_BUILD_ATTEMPT_1
        with self.assertRaises(SimulatedProcessKill):
            orch.run(
                month="2025-01",
                parquet_path=self.p_2025,
                zone_lookup_path=self.zones_path,
                run_id=run_id,
                reset_state=True,
                simulate_kill_after="STAGE_2_BUILD_ATTEMPT_1",
            )

        # Step B: Resume without reset_state and verify completed stages are skipped
        resumed_trace = orch.run(
            month="2025-01",
            parquet_path=self.p_2025,
            zone_lookup_path=self.zones_path,
            run_id=run_id,
            reset_state=False,
            trace_alias="trace_4_durable_resume_2025-01",
        )
        self.assertEqual(resumed_trace.status, RunStatus.ACCEPTED)
        self.assertTrue(resumed_trace.resumed_from_checkpoint)
        self.assertIn("STAGE_1_PROFILE_AND_DRIFT", resumed_trace.skipped_stages)
        self.assertIn("STAGE_2_BUILD_ATTEMPT_1", resumed_trace.skipped_stages)

    def test_5_refusal_on_corrupted_source_2025_99(self) -> None:
        orch = RevenueSquadOrchestrator(root_dir=ROOT_DIR)
        trace = orch.run(
            month="2025-99",
            parquet_path=self.p_refusal,
            zone_lookup_path=self.zones_path,
            run_id="test_refusal_2025_99",
            reset_state=True,
        )
        self.assertEqual(trace.status, RunStatus.REFUSED)
        self.assertIsNone(trace.final_output_csv)
        self.assertFalse((ROOT_DIR / "outputs" / "revenue_summary_2025-99.csv").exists())
        self.assertIsNotNone(trace.refusal_report)
        self.assertIn("DRIFT_FATAL_UNMAPPED_PAYMENT_TYPES", trace.refusal_report.violated_contracts)
        self.assertIn("DRIFT_FATAL_ACCOUNTING_RECONCILIATION_FAILURE", trace.refusal_report.violated_contracts)

    def test_6_refusal_on_retry_exhaustion(self) -> None:
        # Force max_retries=1 on 2025-01 where Attempt 1 fails V4 accounting identity check
        orch = RevenueSquadOrchestrator(root_dir=ROOT_DIR, max_retries=1)
        trace = orch.run(
            month="2025-01",
            parquet_path=self.p_2025,
            zone_lookup_path=self.zones_path,
            run_id="test_retry_exhaustion_refusal",
            reset_state=True,
            trace_alias="trace_5_retry_exhaustion_refusal",
        )
        self.assertEqual(trace.status, RunStatus.REFUSED)
        self.assertIsNotNone(trace.refusal_report)
        self.assertEqual(trace.refusal_report.reason_code, "REFUSE_MAX_RETRIES_EXHAUSTED")

        # Re-run clean 2025-01 afterwards so outputs/revenue_summary_2025-01.csv is restored
        orch_full = RevenueSquadOrchestrator(root_dir=ROOT_DIR, max_retries=3)
        orch_full.run(
            month="2025-01",
            parquet_path=self.p_2025,
            zone_lookup_path=self.zones_path,
            run_id="run_2025-01",
            reset_state=True,
            trace_alias="trace_2_drift_and_repair_2025-01",
        )

    def test_7_sql_readonly_security_guardrail(self) -> None:
        engine = SQLDataEngine(self.p_2024, self.zones_path)
        try:
            with self.assertRaises(PermissionError):
                engine.execute_readonly_sql("DROP TABLE yellow_tripdata;")
            with self.assertRaises(PermissionError):
                engine.execute_readonly_sql("DELETE FROM yellow_tripdata WHERE 1=1;")
            with self.assertRaises(PermissionError):
                engine.execute_readonly_sql("SELECT * FROM yellow_tripdata; DROP TABLE taxi_zone_lookup;")
        finally:
            engine.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
