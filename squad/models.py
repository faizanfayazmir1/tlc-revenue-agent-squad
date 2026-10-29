"""
Pydantic data models, contracts, and strict architectural boundary schemas
for the Spyne Monthly Revenue Agent Squad.
Supports both Pydantic v1 and Pydantic v2 seamlessly.
"""

from __future__ import annotations

from enum import Enum
import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class VerifierIsolationError(RuntimeError):
    """Raised if builder reasoning, SQL, or internal state is passed to the Verifier."""


class _CompatModel(BaseModel):
    """Cross-version Pydantic v1 / v2 base model."""

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        if hasattr(super(), "model_dump"):
            return super().model_dump(*args, **kwargs)  # type: ignore[misc]
        return json.loads(self.json(*args, **kwargs))

    def model_dump_json(self, indent: int = 2, **kwargs: Any) -> str:
        if hasattr(super(), "model_dump_json"):
            return super().model_dump_json(indent=indent, **kwargs)  # type: ignore[misc]
        return json.dumps(json.loads(self.json(**kwargs)), indent=indent)


class DriftSeverity(str, Enum):
    CLEAN = "CLEAN"
    ADAPTABLE_DRIFT = "ADAPTABLE_DRIFT"
    FATAL_REFUSAL = "FATAL_REFUSAL"


class RunStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    ACCEPTED = "ACCEPTED"
    REFUSED = "REFUSED"
    ESCALATED = "ESCALATED"


class ColumnProfile(_CompatModel):
    name: str
    arrow_type: str
    logical_type: str
    null_count: int
    null_ratio: float
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None


class TableProfile(_CompatModel):
    month: str
    parquet_path: str
    total_rows: int
    in_month_rows: int
    out_of_month_rows: int
    in_month_ratio: float
    min_pickup_ts: Optional[str] = None
    max_pickup_ts: Optional[str] = None
    columns: Dict[str, ColumnProfile]
    payment_type_counts: Dict[str, int]
    unmapped_payment_type_rows: int
    unmapped_payment_type_ratio: float
    negative_or_zero_total_rows: int
    accounting_mismatch_rows: int
    accounting_mismatch_ratio: float
    max_accounting_discrepancy_usd: float
    detected_fee_columns: List[str]


class DriftFinding(_CompatModel):
    drift_id: str
    category: str
    severity: DriftSeverity
    column_or_field: str
    baseline_expectation: str
    observed_reality: str
    impact_on_revenue: str
    proposed_action: str


class AdaptationPlan(_CompatModel):
    can_proceed: bool
    column_mapping: Dict[str, str] = Field(
        default_factory=dict,
        description="Maps canonical baseline column names to actual file column names.",
    )
    active_surcharge_columns: List[str] = Field(
        default_factory=list,
        description="Actual fee/surcharge columns included in surcharges_and_fees_usd and itemized check.",
    )
    payment_type_mapping: Dict[int, str] = Field(
        default_factory=dict,
        description="Authorized mapping of integer payment_type codes to labels.",
    )
    requires_null_coalesce_on_fees: bool = False
    requires_strict_temporal_filter: bool = True
    notes: List[str] = Field(default_factory=list)


class DriftAssessment(_CompatModel):
    month: str
    overall_severity: DriftSeverity
    decision: str
    findings: List[DriftFinding]
    adaptation_plan: AdaptationPlan
    summary_rationale: str


class BuilderAttemptResult(_CompatModel):
    attempt_number: int
    sql_query: str
    builder_reasoning: str
    applied_fixes: List[str]
    summary_csv_path: str
    summary_json_path: str
    output_row_count: int
    total_gross_revenue_usd: float
    total_net_revenue_usd: float
    execution_ms: float
    estimated_cost_usd: float


class VerifierInput(_CompatModel):
    """
    Hard architectural firewall for the Independent Verifier.
    Forbids any extra fields (such as `builder_reasoning`, `sql_query`,
    `drift_assessment`, or orchestrator history) across both Pydantic v1 and v2.
    """

    class Config:
        extra = "forbid"

    month: str
    attempt_number: int
    summary_csv_path: str
    summary_json_path: str
    contract_path: str
    parquet_path: str
    zone_lookup_path: str
    active_surcharge_columns: List[str]
    column_mapping: Dict[str, str]


class VerificationCheckResult(_CompatModel):
    check_id: str
    check_name: str
    passed: bool
    expected_contract: str
    observed_evidence: str
    severity: str = "CRITICAL"
    remediation_hint: Optional[str] = None


class VerificationReport(_CompatModel):
    month: str
    attempt_number: int
    passed: bool
    decision: str
    checks: List[VerificationCheckResult]
    rejection_codes: List[str]
    remediation_hints: List[str]
    independent_audit_metrics: Dict[str, Any]
    evidence_file_path: str


class RefusalReport(_CompatModel):
    month: str
    run_id: str
    refusal_stage: str
    reason_code: str
    headline: str
    detailed_explanation: str
    violated_contracts: List[str]
    evidence_metrics: Dict[str, Any]
    human_escalation_action: str


class TraceEvent(_CompatModel):
    seq_no: int
    timestamp: str
    agent: str
    stage: str
    action: str
    tool_used: Optional[str] = None
    status: str
    summary: str
    details: Dict[str, Any] = Field(default_factory=dict)


class RunTrace(_CompatModel):
    run_id: str
    month: str
    parquet_path: str
    status: RunStatus
    steps_used: int
    max_steps: int
    retries_used: int
    max_retries: int
    total_cost_usd: float
    max_cost_usd: float
    resumed_from_checkpoint: bool = False
    skipped_stages: List[str] = Field(default_factory=list)
    drift_assessment: Optional[DriftAssessment] = None
    builder_attempts: List[BuilderAttemptResult] = Field(default_factory=list)
    verification_reports: List[VerificationReport] = Field(default_factory=list)
    refusal_report: Optional[RefusalReport] = None
    final_output_csv: Optional[str] = None
    final_output_json: Optional[str] = None
    final_verifier_evidence: Optional[str] = None
    events: List[TraceEvent] = Field(default_factory=list)
