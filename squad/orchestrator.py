"""
Orchestrator (`RevenueSquadOrchestrator`) for the Spyne Monthly Revenue Agent Squad.
Coordinates:
  - Agent 1: `SourceProfilerAndDriftAgent` (schema/data profiling & drift classification)
  - Agent 2: `SQLRevenueBuilderAgent` (SQL synthesis, execution & verifier-driven repair)
  - Agent 3: `IndependentVerifierAgent` (isolated contract verification & raw SQL audit)

Enforces:
  - Durable state checkpointing via SQLite WAL (`DurableStateStore`) so a killed
    process resumes mid-run without redoing completed stages.
  - Strict step count (`max_steps`), retry count (`max_retries`), and budget (`max_cost_usd`).
  - Explicit refusal path when fatal drift is detected or when repair retries are exhausted.
  - Detailed JSON and Markdown audit traces showing who did what, what was checked,
    and why the output was accepted or refused.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Dict, List, Optional

from squad.builder_agent import SQLRevenueBuilderAgent
from squad.models import (
    BuilderAttemptResult,
    DriftAssessment,
    DriftSeverity,
    RefusalReport,
    RunStatus,
    RunTrace,
    TableProfile,
    VerificationReport,
)
from squad.profiler_agent import SourceProfilerAndDriftAgent
from squad.sql_engine import SQLDataEngine
from squad.state_store import DurableStateStore
from squad.verifier_agent import IndependentVerifierAgent


class SimulatedProcessKill(SystemExit):
    """Raised when `--simulate-kill-after` triggers an intentional mid-run crash test."""


class RevenueSquadOrchestrator:
    """Stateful orchestrator managing the 3-agent monthly revenue squad."""

    ORCHESTRATOR_NAME = "RevenueSquadOrchestrator"

    def __init__(
        self,
        root_dir: Path,
        max_steps: int = 12,
        max_retries: int = 3,
        max_cost_usd: float = 0.25,
    ) -> None:
        self.root_dir = Path(root_dir)
        self.contracts_dir = self.root_dir / "contracts"
        self.outputs_dir = self.root_dir / "outputs"
        self.traces_dir = self.root_dir / "traces"
        self.state_dir = self.root_dir / "state"

        self.outputs_dir.mkdir(parents=True, exist_ok=True)
        self.traces_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self.baseline_contract_path = self.contracts_dir / "schema_baseline_2024.json"
        self.revenue_contract_path = self.contracts_dir / "revenue_contract.json"

        self.max_steps = max_steps
        self.max_retries = max_retries
        self.max_cost_usd = max_cost_usd

        self.state_store = DurableStateStore(self.state_dir / "squad_state.db")
        self.profiler_agent = SourceProfilerAndDriftAgent(
            self.baseline_contract_path,
            self.revenue_contract_path,
        )
        self.builder_agent = SQLRevenueBuilderAgent(self.outputs_dir)
        self.verifier_agent = IndependentVerifierAgent(self.outputs_dir)

    def _check_bounds(self, run_id: str, month: str) -> Optional[RefusalReport]:
        counters = self.state_store.get_run_counters(run_id)
        if counters["steps_used"] >= self.max_steps:
            return RefusalReport(
                month=month,
                run_id=run_id,
                refusal_stage="ORCHESTRATOR_BOUNDS_CHECK",
                reason_code="REFUSE_STEP_BUDGET_EXCEEDED",
                headline=f"Step count budget exhausted ({counters['steps_used']} >= {self.max_steps}).",
                detailed_explanation="The squad reached its hard step ceiling without converging on a verified summary.",
                violated_contracts=["execution_bounds.max_steps"],
                evidence_metrics=counters,
                human_escalation_action="Inspect trace for recurring verification failures and review source schema.",
            )
        if counters["cost_usd"] >= self.max_cost_usd:
            return RefusalReport(
                month=month,
                run_id=run_id,
                refusal_stage="ORCHESTRATOR_BOUNDS_CHECK",
                reason_code="REFUSE_COST_BUDGET_EXCEEDED",
                headline=f"Cost budget exhausted (${counters['cost_usd']:.4f} >= ${self.max_cost_usd:.4f}).",
                detailed_explanation="The squad reached its maximum USD cost ceiling and halted execution.",
                violated_contracts=["execution_bounds.max_cost_usd"],
                evidence_metrics=counters,
                human_escalation_action="Approve higher cost budget or fix underlying data corruption.",
            )
        return None

    def run(
        self,
        month: str,
        parquet_path: Path,
        zone_lookup_path: Path,
        run_id: Optional[str] = None,
        reset_state: bool = False,
        simulate_kill_after: Optional[str] = None,
        force_uncoalesced_attempt_1: bool = False,
        force_unfiltered_dates_attempt_1: bool = False,
        trace_alias: Optional[str] = None,
    ) -> RunTrace:
        effective_run_id = run_id or f"run_{month}"
        run_meta = self.state_store.init_or_load_run(
            run_id=effective_run_id,
            month=month,
            parquet_path=str(parquet_path),
            reset=reset_state,
        )
        resumed = bool(run_meta["resumed"])
        skipped_stages: List[str] = []

        self.state_store.append_trace_event(
            run_id=effective_run_id,
            agent=self.ORCHESTRATOR_NAME,
            stage="STAGE_0_INIT",
            action="INITIALIZE_OR_RESUME_RUN",
            tool_used="DurableStateStore.init_or_load_run",
            status="RESUMED" if resumed else "STARTED",
            summary=(
                f"{'Resumed existing durable run' if resumed else 'Started new run'} '{effective_run_id}' "
                f"for month={month} (bounds: max_steps={self.max_steps}, max_retries={self.max_retries}, "
                f"max_cost_usd=${self.max_cost_usd:.2f})."
            ),
            details={"parquet_path": str(parquet_path), "resumed": resumed},
        )

        engine = SQLDataEngine(parquet_path=parquet_path, zone_lookup_path=zone_lookup_path)
        try:
            # =================================================================
            # STAGE 1: Source Profiling & Drift Detection
            # =================================================================
            stage_1_key = "STAGE_1_PROFILE_AND_DRIFT"
            cp_stage_1 = self.state_store.get_checkpoint(effective_run_id, stage_1_key)
            if cp_stage_1 is not None:
                skipped_stages.append(stage_1_key)
                profile = TableProfile(**cp_stage_1["profile"])
                drift_assessment = DriftAssessment(**cp_stage_1["drift_assessment"])
                self.state_store.append_trace_event(
                    run_id=effective_run_id,
                    agent=self.ORCHESTRATOR_NAME,
                    stage=stage_1_key,
                    action="SKIP_COMPLETED_STAGE",
                    tool_used="DurableStateStore.get_checkpoint",
                    status="SKIPPED_FROM_DURABLE_STATE",
                    summary=(
                        f"Loaded {stage_1_key} from durable SQLite checkpoint without re-profiling "
                        f"({profile.total_rows:,} rows, decision={drift_assessment.decision})."
                    ),
                )
            else:
                profile, drift_assessment = self.profiler_agent.profile_and_detect_drift(month, engine)
                self.state_store.save_checkpoint(
                    run_id=effective_run_id,
                    stage_key=stage_1_key,
                    payload={
                        "profile": profile.model_dump(),
                        "drift_assessment": drift_assessment.model_dump(),
                    },
                    steps_delta=1,
                    retries_delta=0,
                    cost_delta_usd=0.008,
                )
                self.state_store.append_trace_event(
                    run_id=effective_run_id,
                    agent=self.profiler_agent.AGENT_NAME,
                    stage=stage_1_key,
                    action="PROFILE_SOURCE_AND_DETECT_DRIFT",
                    tool_used="inspect_parquet_schema + execute_readonly_sql",
                    status=drift_assessment.decision,
                    summary=drift_assessment.summary_rationale,
                    details={
                        "total_rows": profile.total_rows,
                        "in_month_rows": profile.in_month_rows,
                        "out_of_month_rows": profile.out_of_month_rows,
                        "detected_fee_columns": profile.detected_fee_columns,
                        "payment_type_counts": profile.payment_type_counts,
                        "accounting_mismatch_rows": profile.accounting_mismatch_rows,
                        "accounting_mismatch_ratio": profile.accounting_mismatch_ratio,
                        "drift_findings": [f.model_dump() for f in drift_assessment.findings],
                    },
                )
                if simulate_kill_after == stage_1_key:
                    self.state_store.append_trace_event(
                        run_id=effective_run_id,
                        agent=self.ORCHESTRATOR_NAME,
                        stage=stage_1_key,
                        action="SIMULATE_PROCESS_KILL",
                        status="KILLED",
                        summary=f"Process intentionally killed after {stage_1_key} to test durable state resumption.",
                    )
                    raise SimulatedProcessKill(f"Simulated mid-run kill after {stage_1_key}")

            # Check if Drift Agent decided to STOP_AND_REFUSE
            if drift_assessment.overall_severity == DriftSeverity.FATAL_REFUSAL or not drift_assessment.adaptation_plan.can_proceed:
                fatal_findings = [
                    f for f in drift_assessment.findings if f.severity == DriftSeverity.FATAL_REFUSAL
                ]
                refusal = RefusalReport(
                    month=month,
                    run_id=effective_run_id,
                    refusal_stage="STAGE_1_DRIFT_DETECTION_GATE",
                    reason_code=fatal_findings[0].drift_id if fatal_findings else "REFUSE_FATAL_SOURCE_DRIFT",
                    headline=f"Refused to ship {month} revenue summary due to fatal source data/contract violation.",
                    detailed_explanation=drift_assessment.summary_rationale,
                    violated_contracts=[f.drift_id for f in fatal_findings],
                    evidence_metrics={
                        "total_rows": profile.total_rows,
                        "in_month_ratio": profile.in_month_ratio,
                        "unmapped_payment_type_rows": profile.unmapped_payment_type_rows,
                        "unmapped_payment_type_ratio": profile.unmapped_payment_type_ratio,
                        "accounting_mismatch_rows": profile.accounting_mismatch_rows,
                        "accounting_mismatch_ratio": profile.accounting_mismatch_ratio,
                        "max_accounting_discrepancy_usd": profile.max_accounting_discrepancy_usd,
                    },
                    human_escalation_action=(
                        "Escalate to Finance & TLC Vendor Data Engineering: do not ingest or report this monthly file "
                        "until the corrupted `total_amount` accounting discrepancy and unmapped `payment_type` codes are fixed upstream."
                    ),
                )
                return self._finalize_refusal(
                    run_id=effective_run_id,
                    month=month,
                    parquet_path=str(parquet_path),
                    resumed=resumed,
                    skipped_stages=skipped_stages,
                    drift_assessment=drift_assessment,
                    builder_attempts=[],
                    verification_reports=[],
                    refusal=refusal,
                    trace_alias=trace_alias,
                )

            # =================================================================
            # STAGES 2 & 3: Bounded Build -> Independent Verify -> Repair Loop
            # =================================================================
            builder_attempts: List[BuilderAttemptResult] = []
            verification_reports: List[VerificationReport] = []
            prior_verification: Optional[VerificationReport] = None

            for attempt in range(1, self.max_retries + 1):
                bounds_refusal = self._check_bounds(effective_run_id, month)
                if bounds_refusal is not None:
                    return self._finalize_refusal(
                        run_id=effective_run_id,
                        month=month,
                        parquet_path=str(parquet_path),
                        resumed=resumed,
                        skipped_stages=skipped_stages,
                        drift_assessment=drift_assessment,
                        builder_attempts=builder_attempts,
                        verification_reports=verification_reports,
                        refusal=bounds_refusal,
                        trace_alias=trace_alias,
                    )

                # --- Builder Stage ---
                build_stage_key = f"STAGE_2_BUILD_ATTEMPT_{attempt}"
                cp_build = self.state_store.get_checkpoint(effective_run_id, build_stage_key)
                if cp_build is not None:
                    skipped_stages.append(build_stage_key)
                    b_res = BuilderAttemptResult(**cp_build["builder_attempt"])
                    builder_attempts.append(b_res)
                    self.state_store.append_trace_event(
                        run_id=effective_run_id,
                        agent=self.ORCHESTRATOR_NAME,
                        stage=build_stage_key,
                        action="SKIP_COMPLETED_STAGE",
                        tool_used="DurableStateStore.get_checkpoint",
                        status="SKIPPED_FROM_DURABLE_STATE",
                        summary=(
                            f"Loaded {build_stage_key} from durable SQLite checkpoint "
                            f"(gross_passenger_revenue_usd=${b_res.total_gross_revenue_usd:,.2f})."
                        ),
                    )
                else:
                    b_res = self.builder_agent.build_summary(
                        month=month,
                        attempt_number=attempt,
                        profile=profile,
                        drift_assessment=drift_assessment,
                        engine=engine,
                        prior_verification=prior_verification,
                        force_uncoalesced_attempt_1=force_uncoalesced_attempt_1,
                        force_unfiltered_dates_attempt_1=force_unfiltered_dates_attempt_1,
                    )
                    builder_attempts.append(b_res)
                    self.state_store.save_checkpoint(
                        run_id=effective_run_id,
                        stage_key=build_stage_key,
                        payload={"builder_attempt": b_res.model_dump()},
                        steps_delta=1,
                        retries_delta=(1 if attempt > 1 else 0),
                        cost_delta_usd=b_res.estimated_cost_usd,
                    )
                    self.state_store.append_trace_event(
                        run_id=effective_run_id,
                        agent=self.builder_agent.AGENT_NAME,
                        stage=build_stage_key,
                        action="BUILD_REVENUE_SUMMARY_SQL",
                        tool_used="execute_readonly_sql",
                        status="BUILT_CANDIDATE",
                        summary=b_res.builder_reasoning,
                        details={
                            "attempt_number": attempt,
                            "output_row_count": b_res.output_row_count,
                            "total_gross_revenue_usd": b_res.total_gross_revenue_usd,
                            "total_net_revenue_usd": b_res.total_net_revenue_usd,
                            "applied_fixes": b_res.applied_fixes,
                            "execution_ms": b_res.execution_ms,
                            "sql_query": b_res.sql_query,
                        },
                    )
                    if simulate_kill_after == build_stage_key:
                        self.state_store.append_trace_event(
                            run_id=effective_run_id,
                            agent=self.ORCHESTRATOR_NAME,
                            stage=build_stage_key,
                            action="SIMULATE_PROCESS_KILL",
                            status="KILLED",
                            summary=f"Process intentionally killed after {build_stage_key} to test durable state resumption.",
                        )
                        raise SimulatedProcessKill(f"Simulated mid-run kill after {build_stage_key}")

                # --- Independent Verifier Stage ---
                verify_stage_key = f"STAGE_3_VERIFY_ATTEMPT_{attempt}"
                cp_verify = self.state_store.get_checkpoint(effective_run_id, verify_stage_key)
                if cp_verify is not None:
                    skipped_stages.append(verify_stage_key)
                    v_rep = VerificationReport(**cp_verify["verification_report"])
                    verification_reports.append(v_rep)
                    self.state_store.append_trace_event(
                        run_id=effective_run_id,
                        agent=self.ORCHESTRATOR_NAME,
                        stage=verify_stage_key,
                        action="SKIP_COMPLETED_STAGE",
                        tool_used="DurableStateStore.get_checkpoint",
                        status="SKIPPED_FROM_DURABLE_STATE",
                        summary=(
                            f"Loaded {verify_stage_key} from durable SQLite checkpoint "
                            f"(decision={v_rep.decision}, checks_passed={v_rep.independent_audit_metrics.get('checks_passed')}/{len(v_rep.checks)})."
                        ),
                    )
                else:
                    # Construct strictly isolated VerifierInput dict (NO builder_reasoning, NO sql_query!)
                    isolated_verifier_payload = {
                        "month": month,
                        "attempt_number": attempt,
                        "summary_csv_path": b_res.summary_csv_path,
                        "summary_json_path": b_res.summary_json_path,
                        "contract_path": str(self.revenue_contract_path),
                        "parquet_path": str(parquet_path),
                        "zone_lookup_path": str(zone_lookup_path),
                        "active_surcharge_columns": drift_assessment.adaptation_plan.active_surcharge_columns,
                        "column_mapping": drift_assessment.adaptation_plan.column_mapping,
                    }
                    v_rep = self.verifier_agent.verify(isolated_verifier_payload, engine)
                    verification_reports.append(v_rep)
                    self.state_store.save_checkpoint(
                        run_id=effective_run_id,
                        stage_key=verify_stage_key,
                        payload={"verification_report": v_rep.model_dump()},
                        steps_delta=1,
                        retries_delta=0,
                        cost_delta_usd=0.009,
                    )
                    self.state_store.append_trace_event(
                        run_id=effective_run_id,
                        agent=self.verifier_agent.AGENT_NAME,
                        stage=verify_stage_key,
                        action="INDEPENDENT_CONTRACT_VERIFICATION",
                        tool_used="execute_readonly_sql (independent audit queries V1..V8)",
                        status=v_rep.decision,
                        summary=(
                            f"Verifier {v_rep.decision}ED Attempt {attempt} for {month}: "
                            f"{v_rep.independent_audit_metrics['checks_passed']}/{v_rep.independent_audit_metrics['checks_total']} "
                            f"checks passed. Rejection codes: {v_rep.rejection_codes or 'None'}."
                        ),
                        details={
                            "attempt_number": attempt,
                            "decision": v_rep.decision,
                            "rejection_codes": v_rep.rejection_codes,
                            "remediation_hints": v_rep.remediation_hints,
                            "independent_audit_metrics": v_rep.independent_audit_metrics,
                            "checks": [c.model_dump() for c in v_rep.checks],
                        },
                    )
                    if simulate_kill_after == verify_stage_key:
                        self.state_store.append_trace_event(
                            run_id=effective_run_id,
                            agent=self.ORCHESTRATOR_NAME,
                            stage=verify_stage_key,
                            action="SIMULATE_PROCESS_KILL",
                            status="KILLED",
                            summary=f"Process intentionally killed after {verify_stage_key} to test durable state resumption.",
                        )
                        raise SimulatedProcessKill(f"Simulated mid-run kill after {verify_stage_key}")

                if v_rep.passed:
                    # Success! Finalize accepted run
                    self.state_store.update_run_status(effective_run_id, RunStatus.ACCEPTED)
                    counters = self.state_store.get_run_counters(effective_run_id)
                    self.state_store.append_trace_event(
                        run_id=effective_run_id,
                        agent=self.ORCHESTRATOR_NAME,
                        stage="STAGE_4_FINALIZE_ACCEPTED",
                        action="PUBLISH_VERIFIED_REVENUE_SUMMARY",
                        status="ACCEPTED",
                        summary=(
                            f"Run '{effective_run_id}' ACCEPTED on Attempt {attempt}. "
                            f"Verified gross_passenger_revenue_usd=${b_res.total_gross_revenue_usd:,.2f} "
                            f"across {b_res.output_row_count} (pickup_zone, payment_type) groups."
                        ),
                        details={
                            "final_output_csv": b_res.summary_csv_path,
                            "final_output_json": b_res.summary_json_path,
                            "final_verifier_evidence": v_rep.evidence_file_path,
                            "steps_used": counters["steps_used"],
                            "retries_used": counters["retries_used"],
                            "total_cost_usd": counters["cost_usd"],
                        },
                    )
                    trace = RunTrace(
                        run_id=effective_run_id,
                        month=month,
                        parquet_path=str(parquet_path),
                        status=RunStatus.ACCEPTED,
                        steps_used=counters["steps_used"],
                        max_steps=self.max_steps,
                        retries_used=counters["retries_used"],
                        max_retries=self.max_retries,
                        total_cost_usd=counters["cost_usd"],
                        max_cost_usd=self.max_cost_usd,
                        resumed_from_checkpoint=resumed,
                        skipped_stages=skipped_stages,
                        drift_assessment=drift_assessment,
                        builder_attempts=builder_attempts,
                        verification_reports=verification_reports,
                        refusal_report=None,
                        final_output_csv=b_res.summary_csv_path,
                        final_output_json=b_res.summary_json_path,
                        final_verifier_evidence=v_rep.evidence_file_path,
                        events=self.state_store.list_trace_events(effective_run_id),
                    )
                    self._write_trace_files(trace, trace_alias=trace_alias)
                    return trace

                # Otherwise, Verifier rejected this attempt; loop to next repair attempt if within max_retries
                prior_verification = v_rep

            # If loop finishes without passing verification, retries are exhausted -> escalate & refuse!
            last_v = verification_reports[-1]
            refusal = RefusalReport(
                month=month,
                run_id=effective_run_id,
                refusal_stage="STAGE_3_VERIFICATION_RETRY_EXHAUSTION",
                reason_code="REFUSE_MAX_RETRIES_EXHAUSTED",
                headline=(
                    f"Verifier rejected all {self.max_retries} Builder attempts for {month}. "
                    "Refusing to ship unverified revenue numbers and escalating to human review."
                ),
                detailed_explanation=(
                    f"After {self.max_retries} bounded repair iterations, candidate output still violated "
                    f"contract checks: {last_v.rejection_codes}."
                ),
                violated_contracts=last_v.rejection_codes,
                evidence_metrics=last_v.independent_audit_metrics,
                human_escalation_action=(
                    "Escalate to Finance & Data Engineering with attached trace and Verifier rejection report."
                ),
            )
            return self._finalize_refusal(
                run_id=effective_run_id,
                month=month,
                parquet_path=str(parquet_path),
                resumed=resumed,
                skipped_stages=skipped_stages,
                drift_assessment=drift_assessment,
                builder_attempts=builder_attempts,
                verification_reports=verification_reports,
                refusal=refusal,
                trace_alias=trace_alias,
            )
        finally:
            engine.close()

    def _finalize_refusal(
        self,
        run_id: str,
        month: str,
        parquet_path: str,
        resumed: bool,
        skipped_stages: List[str],
        drift_assessment: Optional[DriftAssessment],
        builder_attempts: List[BuilderAttemptResult],
        verification_reports: List[VerificationReport],
        refusal: RefusalReport,
        trace_alias: Optional[str] = None,
    ) -> RunTrace:
        self.state_store.update_run_status(run_id, RunStatus.REFUSED)
        # Remove any unverified candidate output files so a wrong number can NEVER be consumed downstream
        candidate_csv = self.outputs_dir / f"revenue_summary_{month}.csv"
        candidate_json = self.outputs_dir / f"revenue_summary_{month}.json"
        if candidate_csv.exists():
            candidate_csv.unlink()
        if candidate_json.exists():
            candidate_json.unlink()

        refusal_path = self.outputs_dir / f"refusal_report_{month}.json"
        refusal_path.write_text(refusal.model_dump_json(indent=2), encoding="utf-8")

        counters = self.state_store.get_run_counters(run_id)
        self.state_store.append_trace_event(
            run_id=run_id,
            agent=self.ORCHESTRATOR_NAME,
            stage="STAGE_4_FINALIZE_REFUSAL",
            action="REFUSE_AND_ESCALATE_TO_HUMAN",
            status="REFUSED",
            summary=f"{refusal.headline} Reason={refusal.reason_code}. Escalation={refusal.human_escalation_action}",
            details=refusal.model_dump(),
        )

        trace = RunTrace(
            run_id=run_id,
            month=month,
            parquet_path=parquet_path,
            status=RunStatus.REFUSED,
            steps_used=counters["steps_used"],
            max_steps=self.max_steps,
            retries_used=counters["retries_used"],
            max_retries=self.max_retries,
            total_cost_usd=counters["cost_usd"],
            max_cost_usd=self.max_cost_usd,
            resumed_from_checkpoint=resumed,
            skipped_stages=skipped_stages,
            drift_assessment=drift_assessment,
            builder_attempts=builder_attempts,
            verification_reports=verification_reports,
            refusal_report=refusal,
            final_output_csv=None,
            final_output_json=None,
            final_verifier_evidence=str(refusal_path),
            events=self.state_store.list_trace_events(run_id),
        )
        self._write_trace_files(trace, trace_alias=trace_alias)
        return trace

    def _write_trace_files(self, trace: RunTrace, trace_alias: Optional[str] = None) -> None:
        base_name = trace_alias or f"trace_{trace.month}"
        json_path = self.traces_dir / f"{base_name}.json"
        md_path = self.traces_dir / f"{base_name}.md"

        json_path.write_text(trace.model_dump_json(indent=2), encoding="utf-8")
        md_path.write_text(self._render_trace_markdown(trace), encoding="utf-8")

    @staticmethod
    def _render_trace_markdown(trace: RunTrace) -> str:
        lines: List[str] = [
            f"# Execution Trace — `{trace.run_id}` (Month: `{trace.month}`)",
            "",
            "| Attribute | Value |",
            "| :--- | :--- |",
            f"| **Final Status** | `{trace.status.value}` |",
            f"| **Source Parquet** | `{trace.parquet_path}` |",
            f"| **Steps Used / Max** | `{trace.steps_used} / {trace.max_steps}` |",
            f"| **Repair Retries Used / Max** | `{trace.retries_used} / {trace.max_retries}` |",
            f"| **Cost Used / Max Budget** | `${trace.total_cost_usd:.4f} / ${trace.max_cost_usd:.2f}` |",
            f"| **Resumed From Checkpoint** | `{trace.resumed_from_checkpoint}` (Skipped stages: `{trace.skipped_stages}`) |",
            f"| **Final Output CSV** | `{trace.final_output_csv or 'NONE (Refused — unverified output purged)'}` |",
            f"| **Verifier Evidence / Refusal** | `{trace.final_verifier_evidence}` |",
            "",
        ]

        if trace.drift_assessment:
            da = trace.drift_assessment
            lines.extend(
                [
                    "## 1. Drift Detection & Source Assessment",
                    f"- **Overall Severity**: `{da.overall_severity.value}`",
                    f"- **Decision**: `{da.decision}`",
                    f"- **Rationale**: {da.summary_rationale}",
                    f"- **Active Surcharge Columns**: `{da.adaptation_plan.active_surcharge_columns}`",
                    "",
                ]
            )
            if da.findings:
                lines.extend(
                    [
                        "| Drift ID | Category | Severity | Field | Baseline vs. Observed | Action |",
                        "| :--- | :--- | :--- | :--- | :--- | :--- |",
                    ]
                )
                for f in da.findings:
                    lines.append(
                        f"| `{f.drift_id}` | `{f.category}` | `{f.severity.value}` | `{f.column_or_field}` | "
                        f"**Expected**: {f.baseline_expectation}<br>**Observed**: {f.observed_reality} | {f.proposed_action} |"
                    )
                lines.append("")

        if trace.builder_attempts:
            lines.append("## 2. SQL Builder & Repair Loop Attempts")
            for b in trace.builder_attempts:
                lines.extend(
                    [
                        f"### Attempt {b.attempt_number}",
                        f"- **Reasoning**: {b.builder_reasoning}",
                        f"- **Applied Fixes**: `{b.applied_fixes}`",
                        f"- **Candidate Gross Passenger Revenue**: `${b.total_gross_revenue_usd:,.2f}` ({b.output_row_count} rows, {b.execution_ms} ms)",
                        "",
                    ]
                )

        if trace.verification_reports:
            lines.append("## 3. Independent Verifier Audit Reports")
            for vr in trace.verification_reports:
                lines.extend(
                    [
                        f"### Verification of Attempt {vr.attempt_number} — Decision: `{vr.decision}`",
                        f"- **Builder Isolation Enforced**: `{vr.independent_audit_metrics.get('isolation_verified')}` (Verifier received only candidate CSV + contract, zero builder reasoning/SQL)",
                        f"- **Rejection Codes**: `{vr.rejection_codes or 'NONE'}`",
                        "",
                        "| Check ID | Check Name | Passed | Expected Contract | Observed Evidence |",
                        "| :--- | :--- | :--- | :--- | :--- |",
                    ]
                )
                for c in vr.checks:
                    status_icon = "PASS" if c.passed else "FAIL"
                    lines.append(
                        f"| `{c.check_id}` | {c.check_name} | **{status_icon}** | {c.expected_contract} | {c.observed_evidence} |"
                    )
                lines.append("")

        if trace.refusal_report:
            rr = trace.refusal_report
            lines.extend(
                [
                    "## 4. Refusal & Human Escalation Report",
                    f"- **Refusal Stage**: `{rr.refusal_stage}`",
                    f"- **Reason Code**: `{rr.reason_code}`",
                    f"- **Headline**: {rr.headline}",
                    f"- **Detailed Explanation**: {rr.detailed_explanation}",
                    f"- **Violated Contracts**: `{rr.violated_contracts}`",
                    f"- **Human Escalation Action**: {rr.human_escalation_action}",
                    "",
                ]
            )

        lines.extend(
            [
                "## 5. Chronological Event Log",
                "",
                "| Seq | Timestamp (UTC) | Agent | Stage | Action | Tool | Status | Summary |",
                "| :---: | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
            ]
        )
        for ev in trace.events:
            lines.append(
                f"| {ev.seq_no} | `{ev.timestamp}` | `{ev.agent}` | `{ev.stage}` | `{ev.action}` | "
                f"`{ev.tool_used or '-'}` | `{ev.status}` | {ev.summary} |"
            )
        lines.append("")
        return "\n".join(lines)
