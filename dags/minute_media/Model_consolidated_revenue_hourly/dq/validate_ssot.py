"""
SSOT data quality validation — runs as the final task after all merges.

SSOT-level checks run every cycle. Pipeline-specific conservation checks
run only when the pipeline's processing task succeeded (STG data is fresh).
"""
import logging
import os
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

log = logging.getLogger("airflow.task")

_SQL_DIR = os.path.join(os.path.dirname(__file__), "tests")

# Task IDs of the last processing step in each pipeline.
# If the task succeeded, the STG table it creates is fresh.
_PIPELINE_GATE_TASKS = {
    "ssp": "ssp_explode_data",
    "syndication": "syndication_explode_data",
    "demand_partner": "demand_partner_allocation",
    "gam": "GAM_allocation",
}

# Pipeline-specific checks — sql file is always {check_name}.sql
_PIPELINE_CHECKS = {
    "ssp": "ssp_revenue_conservation",
    "syndication": "syndication_revenue_conservation",
    "gam": "gam_revenue_bounding",
    "demand_partner": "demand_partner_revenue_bounding",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _pipeline_processed(dag_run, task_id: str) -> bool:
    ti = dag_run.get_task_instance(task_id)
    return ti is not None and ti.state == "success"


def _load_sql(check_name: str) -> str:
    with open(os.path.join(_SQL_DIR, f"{check_name}.sql")) as f:
        return f.read()


def _run_check(client, check_num, total, name, severity, sql):
    """Execute a check query, log the result, and return pass/fail info."""
    rows = list(client.query(sql).result())
    if not rows:
        expected, actual, passed = "0", "0", True
    else:
        row = rows[0]
        passed = bool(row.get("passed", False))
        expected = str(row.get("expected_value", ""))
        actual = str(row.get("actual_value", ""))

    prefix = f"[{check_num}/{total}]"
    if passed:
        log.info("%s [PASS] %s: expected=%s actual=%s", prefix, name, expected, actual)
    elif severity == "blocking":
        log.error("%s [FAIL] %s: expected=%s actual=%s", prefix, name, expected, actual)
    else:
        log.warning("%s [WARN] %s: expected=%s actual=%s", prefix, name, expected, actual)

    return severity if not passed else "pass"


def _log_skip(check_num, total, name):
    log.info("[%d/%d] [SKIP] %s", check_num, total, name)


# ---------------------------------------------------------------------------
# Entry point — called by the PythonOperator
# ---------------------------------------------------------------------------
def validate_ssot(*, gcp_conn_id: str, bq_location: str, **context: Any) -> None:
    dag_run = context["dag_run"]
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=bq_location)
    client = hook.get_client()

    # ── Determine which pipelines produced fresh STG data ─────────────
    pipelines_ran = {"events_stream": True}
    for name, tid in _PIPELINE_GATE_TASKS.items():
        pipelines_ran[name] = _pipeline_processed(dag_run, tid)

    ran = [k for k, v in pipelines_ran.items() if v]
    skipped = [k for k, v in pipelines_ran.items() if not v]
    log.info("Pipelines processed: %s", ", ".join(ran) if ran else "none")
    log.info("Pipelines skipped: %s", ", ".join(skipped) if skipped else "none")

    # ── Count total checks up front for log numbering ─────────────────
    ssot_checks = ["ssot_grain_uniqueness", "ssot_required_fields", "ssot_no_negatives"]
    total = len(ssot_checks) + len(_PIPELINE_CHECKS)
    check_num = 0

    blocking_failures = []
    counters = {"pass": 0, "warning": 0, "skip": 0}

    log.info(f"{'='*10} SSOT Validation Results ({total} checks) {'='*10}")

    # ── SSOT checks (always) ─────────────────────────────────────────
    for check_name in ssot_checks:
        check_num += 1
        result = _run_check(client, check_num, total, check_name, "blocking",
                            _load_sql(check_name))
        if result == "blocking":
            blocking_failures.append(check_name)
        else:
            counters[result] = counters.get(result, 0) + 1

    # ── Conditional pipeline checks ──────────────────────────────────
    for pipeline, check_name in _PIPELINE_CHECKS.items():
        check_num += 1
        if pipelines_ran.get(pipeline):
            result = _run_check(client, check_num, total, check_name, "warning",
                                _load_sql(check_name))
            counters[result] = counters.get(result, 0) + 1
        else:
            _log_skip(check_num, total, check_name)
            counters["skip"] += 1

    log.info("=== %d passed, %d failed, %d warnings, %d skipped ===",
             counters["pass"], len(blocking_failures),
             counters.get("warning", 0), counters["skip"])

    if blocking_failures:
        raise AirflowException(
            f"DQ blocking checks failed: {', '.join(blocking_failures)}"
        )
