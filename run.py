#!/usr/bin/env python3
"""
Single-command CLI entrypoint for the Spyne Monthly Revenue Agent Squad.

Examples:
  # Run against 2024 baseline month (Clean Pass):
  python3 run.py --month 2024-01

  # Run against 2025 month (Schema + Domain Drift Caught & Handled + Verifier Repair Loop):
  python3 run.py --month 2025-01

  # Run against any unseen month (auto-downloads from NYC TLC CloudFront if not cached):
  python3 run.py --month 2025-03

  # Run refusal path on corrupted source month:
  python3 run.py --month 2025-99

  # Test durable state (kill mid-run and resume without redoing finished work):
  python3 run.py --month 2025-01 --run-id durable_demo --reset --simulate-kill-after STAGE_1_PROFILE_AND_DRIFT
  python3 run.py --month 2025-01 --run-id durable_demo
"""

from __future__ import annotations

import argparse
import importlib.abc
import importlib.machinery
import importlib.util
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


class _DirectStatPathFinder(importlib.abc.MetaPathFinder):
    """Fallback finder using direct stat() for virtualized/9p filesystems."""

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

from squad.data_loader import ensure_month_parquet, ensure_zone_lookup
from squad.models import RunStatus
from squad.orchestrator import RevenueSquadOrchestrator, SimulatedProcessKill


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spyne Monthly Revenue Agent Squad — Resilient NYC TLC Revenue Pipeline"
    )
    parser.add_argument(
        "positional_month",
        nargs="?",
        default=None,
        help="Target month in YYYY-MM format (positional shortcut, e.g., 2024-01 or 2025-01)",
    )
    parser.add_argument(
        "--month",
        "-m",
        dest="flag_month",
        default=None,
        help="Target month in YYYY-MM format (e.g., 2024-01, 2025-01, 2025-99)",
    )
    parser.add_argument(
        "--parquet-path",
        default=None,
        help="Optional explicit path to a monthly Parquet file (overrides data/yellow_tripdata_<month>.parquet)",
    )
    parser.add_argument(
        "--zone-lookup-path",
        default=str(ROOT_DIR / "data" / "taxi_zone_lookup.csv"),
        help="Path to NYC TLC taxi_zone_lookup.csv",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Durable run identifier in SQLite state store (defaults to run_<month>)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear any prior durable checkpoints for this run-id before starting",
    )
    parser.add_argument(
        "--simulate-kill-after",
        default=None,
        help="Simulate a mid-run crash immediately after the specified stage (e.g., STAGE_1_PROFILE_AND_DRIFT)",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=12,
        help="Maximum orchestrator steps allowed before refusing (default: 12)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum Builder repair attempts allowed before refusing (default: 3)",
    )
    parser.add_argument(
        "--max-cost-usd",
        type=float,
        default=0.25,
        help="Maximum USD budget allowed before refusing (default: 0.25)",
    )
    parser.add_argument(
        "--trace-alias",
        default=None,
        help="Optional custom filename stem for trace files in traces/ (e.g., trace_1_clean_pass_2024_01)",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force download of the monthly Parquet file from NYC TLC CloudFront",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    month = args.flag_month or args.positional_month
    if not month:
        print("Error: Please specify a target month via `--month YYYY-MM` or positional `YYYY-MM`.", file=sys.stderr)
        return 2

    data_dir = ROOT_DIR / "data"
    zone_lookup_path = Path(args.zone_lookup_path)
    if not zone_lookup_path.exists():
        zone_lookup_path = ensure_zone_lookup(data_dir)

    if args.parquet_path:
        parquet_path = Path(args.parquet_path)
    else:
        parquet_path = ensure_month_parquet(month, data_dir, force_download=args.force_download)

    orchestrator = RevenueSquadOrchestrator(
        root_dir=ROOT_DIR,
        max_steps=args.max_steps,
        max_retries=args.max_retries,
        max_cost_usd=args.max_cost_usd,
    )

    try:
        trace = orchestrator.run(
            month=month,
            parquet_path=parquet_path,
            zone_lookup_path=zone_lookup_path,
            run_id=args.run_id,
            reset_state=args.reset,
            simulate_kill_after=args.simulate_kill_after,
            trace_alias=args.trace_alias,
        )
    except SimulatedProcessKill as exc:
        print(f"\n[SIMULATED-KILL] {exc}")
        print("[SIMULATED-KILL] Re-run the command without `--reset` and without `--simulate-kill-after` to resume from durable SQLite state.")
        return 137

    print("=" * 84)
    print(f"SPYNE MONTHLY REVENUE SQUAD — EXECUTION SUMMARY ({month})")
    print("=" * 84)
    print(f"Run ID              : {trace.run_id}")
    print(f"Status              : {trace.status.value}")
    print(f"Resumed Checkpoint  : {trace.resumed_from_checkpoint} (skipped={trace.skipped_stages})")
    print(f"Steps / Max Steps   : {trace.steps_used} / {trace.max_steps}")
    print(f"Retries / Max       : {trace.retries_used} / {trace.max_retries}")
    print(f"Cost / Max Budget   : ${trace.total_cost_usd:.4f} / ${trace.max_cost_usd:.2f}")

    if trace.drift_assessment:
        print(f"Drift Decision      : {trace.drift_assessment.decision} ({trace.drift_assessment.overall_severity.value})")
        print(f"Drift Findings      : {len(trace.drift_assessment.findings)}")

    if trace.status == RunStatus.ACCEPTED:
        last_b = trace.builder_attempts[-1]
        print(f"Verified Gross Rev  : ${last_b.total_gross_revenue_usd:,.2f} ({last_b.output_row_count} zone-payment groups)")
        print(f"Verified Net Rev    : ${last_b.total_net_revenue_usd:,.2f}")
        print(f"Output CSV          : {trace.final_output_csv}")
        print(f"Output JSON         : {trace.final_output_json}")
        print(f"Verifier Evidence   : {trace.final_verifier_evidence}")
        print("=" * 84)
        return 0
    else:
        print(f"Refusal Headline    : {trace.refusal_report.headline if trace.refusal_report else 'Refused'}")
        print(f"Refusal Reason Code : {trace.refusal_report.reason_code if trace.refusal_report else 'UNKNOWN'}")
        print(f"Refusal Evidence    : {trace.final_verifier_evidence}")
        print("=" * 84)
        return 1


if __name__ == "__main__":
    sys.exit(main())
