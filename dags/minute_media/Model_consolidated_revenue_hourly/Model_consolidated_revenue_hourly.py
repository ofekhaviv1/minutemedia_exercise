import os
from datetime import timedelta
from typing import Any, Optional
import pendulum
from minute_media.Model_consolidated_revenue_hourly.dq.validate_ssot import validate_ssot
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator

dag_folder = os.path.dirname(os.path.abspath(__file__))
sql_root = os.path.join(dag_folder, "fact_consolidated_revenue_hourly")

GAM_SQL_DIR = "GAM"


# =============================================================================
# Helpers
# =============================================================================
def _bq_query_from_file(relative_sql_path: str) -> str:
    """
    Return a templated query string that includes a .sql file from template_searchpath.
    """
    return "{% include '" + relative_sql_path + "' %}"


def _bq_sql_task(
    *,
    task_id: str,
    relative_sql_path: str,
    do_xcom_push: bool = False,
    **kwargs,
) -> BigQueryInsertJobOperator:
    """
    Build a BigQueryInsertJobOperator that executes a SQL file.
    GCP connection and location are centralized via GCP_CONN_ID and BQ_LOCATION.
    """
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": _bq_query_from_file(relative_sql_path),
                "useLegacySql": False,
            }
        },
        do_xcom_push=do_xcom_push,
        **kwargs,
    )


def _bq_get_increment(*, task_id: str, table_id: str, selected_fields: str = "increment_column") -> BigQueryGetDataOperator:
    """
    Build a BigQueryGetDataOperator that reads an increment value from a STG table.
    GCP connection and location are centralized via GCP_CONN_ID and BQ_LOCATION.
    """
    return BigQueryGetDataOperator(
        task_id=task_id,
        dataset_id="minute_media_STG",
        table_id=table_id,
        selected_fields=selected_fields,
        max_results=1,
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
    )


def _extract_job_id(xcom_value: Any) -> Optional[str]:
    """
    BigQueryInsertJobOperator typically XCom-pushes a job_id string.
    Handle a few shapes defensively.
    """
    if xcom_value is None:
        return None
    if isinstance(xcom_value, str):
        return xcom_value
    if isinstance(xcom_value, dict):
        for key in ("job_id", "jobId"):
            if key in xcom_value and isinstance(xcom_value[key], str):
                return xcom_value[key]
    return None


def is_gam_execution_hour(**context: Any) -> bool:
    """Returns True only during the 17:00 UTC run."""
    logical_date = context["logical_date"]
    return logical_date.hour == 17


def continue_if_new_data(
    *,
    check_task_id: str,
    **context: Any,
) -> bool:
    """
    Evaluate the result of `<prefix>_check_new_data.sql`.

    The check task is a BigQueryInsertJobOperator that runs a query returning:
      SELECT ... AS has_new_data

    We pull the BigQuery job_id from XCom, fetch its query results, and return
    a boolean. Returning False causes Airflow to skip downstream tasks in the
    TaskGroup.
    """
    ti = context["ti"]
    job_id = _extract_job_id(ti.xcom_pull(task_ids=check_task_id))
    if not job_id:
        raise ValueError(f"Missing BigQuery job_id in XCom for task '{check_task_id}'")

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
    client = hook.get_client()
    job = client.get_job(job_id=job_id, location=BQ_LOCATION)

    rows = list(job.result(max_results=1))
    if not rows:
        return False

    first_row = rows[0]
    has_new_data = first_row.get("has_new_data")  
    return bool(has_new_data)


# =============================================================================
# DAG Definition
# =============================================================================
default_args = {
    "owner": "ofek_haviv",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

GCP_CONN_ID = "google_cloud_default"
BQ_LOCATION = "US"

with DAG(
    dag_id="Model_consolidated_revenue_hourly",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath=[sql_root],
    tags=["minute_media", "bigquery"],
) as dag:
    # -------------------------------------------------------------------------
    # Group 1: events_stream
    # -------------------------------------------------------------------------
    events_stream_increment = _bq_sql_task(
        task_id="events_stream_increment",
        relative_sql_path="events_stream/events_stream_increment.sql",
        do_xcom_push=True,
    )

    events_stream_get_increment = _bq_get_increment(
        task_id="events_stream_get_increment",
        table_id="events_stream_increment",
    )

    with TaskGroup(group_id="events_stream_processing", prefix_group_id=False) as events_stream_processing:
        events_stream_pull_data = _bq_sql_task(
            task_id="events_stream_pull_data",
            relative_sql_path="events_stream/events_stream_pull_data.sql",
        )

        events_stream_grouping = _bq_sql_task(
            task_id="events_stream_grouping",
            relative_sql_path="events_stream/events_stream_grouping.sql",
        )

        events_stream_pull_data >> events_stream_grouping

    events_stream_merge = _bq_sql_task(
        task_id="events_stream_merge",
        relative_sql_path="events_stream/events_stream_merge.sql",
    )

    events_stream_increment >> events_stream_get_increment >>  events_stream_pull_data >> events_stream_grouping >> events_stream_merge

    # -------------------------------------------------------------------------
    # Group 2: GAM
    # -------------------------------------------------------------------------
    gam_increment = _bq_sql_task(
        task_id="GAM_increment",
        relative_sql_path=f"{GAM_SQL_DIR}/GAM_increment.sql",
        do_xcom_push=True,
    )

    gam_get_increment = _bq_get_increment(
        task_id="gam_get_increment",
        table_id="GAM_increment",
    )

    gam_time_gate = ShortCircuitOperator(
        task_id="gam_time_gate",
        python_callable=is_gam_execution_hour,
        ignore_downstream_trigger_rules=False,
    )

    with TaskGroup(group_id="GAM_processing", prefix_group_id=False) as gam_processing:
        gam_pull_data = _bq_sql_task(
            task_id="GAM_pull_data",
            relative_sql_path=f"{GAM_SQL_DIR}/GAM_pull_data.sql",
        )

        gam_grouping = _bq_sql_task(
            task_id="GAM_grouping",
            relative_sql_path=f"{GAM_SQL_DIR}/GAM_grouping.sql",
        )

        gam_last_data_dwh = _bq_sql_task(
            task_id="GAM_last_data_dwh",
            relative_sql_path=f"{GAM_SQL_DIR}/GAM_last_data_dwh.sql",
        )

        gam_allocation = _bq_sql_task(
            task_id="GAM_allocation",
            relative_sql_path=f"{GAM_SQL_DIR}/GAM_allocation.sql",
        )

        gam_pull_data >> [gam_grouping, gam_last_data_dwh] >> gam_allocation

    gam_merge = _bq_sql_task(
        task_id="GAM_merge",
        relative_sql_path=f"{GAM_SQL_DIR}/GAM_merge.sql",
        trigger_rule="none_failed",
    )

    gam_time_gate >> gam_increment >> gam_get_increment >> gam_processing >> gam_merge

    # -------------------------------------------------------------------------
    # Group 3: demand_partner
    # -------------------------------------------------------------------------
    
    demand_partner_increment = _bq_sql_task(
            task_id="demand_partner_increment",
            relative_sql_path="demand_partner/demand_partner_increment.sql",
            do_xcom_push=True,
        )

    
    demand_partner_get_increment = _bq_get_increment(
        task_id="demand_partner_get_increment",
        table_id="demand_partner_increment",
    )


    with TaskGroup(group_id="demand_partner_run_ind", prefix_group_id=False) as demand_partner_run_ind:
        demand_partner_check_new_data = _bq_sql_task(
            task_id="demand_partner_check_new_data",
            relative_sql_path="demand_partner/demand_partner_check_new_data.sql",
            do_xcom_push=True,
        )

        demand_partner_continue_if_new_data = ShortCircuitOperator(
            task_id="demand_partner_continue_if_new_data",
            python_callable=continue_if_new_data,
            op_kwargs={
                "check_task_id": "demand_partner_check_new_data",
            },
            ignore_downstream_trigger_rules=False,
        )

        demand_partner_check_new_data >> demand_partner_continue_if_new_data
    
    demand_partner_check_new_data_get_increment = _bq_get_increment(
        task_id="demand_partner_check_new_data_get_increment",
        table_id="demand_partner_check_new_data",
    )

    with TaskGroup(group_id="demand_partner_processing", prefix_group_id=False) as demand_partner_processing:
        demand_partner_pull_data = _bq_sql_task(
            task_id="demand_partner_pull_data",
            relative_sql_path="demand_partner/demand_partner_pull_data.sql",
        )

        demand_partner_grouping = _bq_sql_task(
            task_id="demand_partner_grouping",
            relative_sql_path="demand_partner/demand_partner_grouping.sql",
        )

        demand_partner_last_data_dwh = _bq_sql_task(
            task_id="demand_partner_last_data_dwh",
            relative_sql_path="demand_partner/demand_partner_last_data_dwh.sql",
        )

        demand_partner_allocation = _bq_sql_task(
            task_id="demand_partner_allocation",
            relative_sql_path="demand_partner/demand_partner_allocation.sql",
        )

        demand_partner_pull_data >> [demand_partner_grouping, demand_partner_last_data_dwh] >> demand_partner_allocation

    demand_partner_merge = _bq_sql_task(
        task_id="demand_partner_merge",
        relative_sql_path="demand_partner/demand_partner_merge.sql",
        trigger_rule="none_failed",
    )

    demand_partner_increment >> demand_partner_get_increment >> demand_partner_run_ind >> demand_partner_check_new_data_get_increment >> demand_partner_processing >> demand_partner_merge

    # -------------------------------------------------------------------------
    # Group 4: SSP
    # -------------------------------------------------------------------------
    ssp_increment = _bq_sql_task(
        task_id="ssp_increment",
        relative_sql_path="SSP/ssp_increment.sql",
        do_xcom_push=True,
    )

    ssp_get_increment = _bq_get_increment(
        task_id="ssp_get_increment",
        table_id="ssp_increment",
    )

    with TaskGroup(group_id="SSP_run_ind", prefix_group_id=False) as ssp_run_ind:
        ssp_check_new_data = _bq_sql_task(
            task_id="ssp_check_new_data",
            relative_sql_path="SSP/ssp_check_new_data.sql",
            do_xcom_push=True,
        )

        ssp_continue_if_new_data = ShortCircuitOperator(
            task_id="ssp_continue_if_new_data",
            python_callable=continue_if_new_data,
            op_kwargs={
                "check_task_id": "ssp_check_new_data",
            },
            ignore_downstream_trigger_rules=False,
        )

        ssp_check_new_data >> ssp_continue_if_new_data

    with TaskGroup(group_id="SSP_processing", prefix_group_id=False) as ssp_processing:
        ssp_pull_data = _bq_sql_task(
            task_id="ssp_pull_data",
            relative_sql_path="SSP/ssp_pull_data.sql",
        )

        ssp_explode_data = _bq_sql_task(
            task_id="ssp_explode_data",
            relative_sql_path="SSP/ssp_explode_data.sql",
        )

        ssp_pull_data >> ssp_explode_data

    ssp_merge = _bq_sql_task(
        task_id="ssp_merge",
        relative_sql_path="SSP/ssp_merge.sql",
    )

    ssp_increment >> ssp_get_increment >> ssp_run_ind >> ssp_processing >> ssp_merge

    # -------------------------------------------------------------------------
    # Group 5: syndication
    # -------------------------------------------------------------------------
    syndication_increment = _bq_sql_task(
        task_id="syndication_increment",
        relative_sql_path="syndication/syndication_increment.sql",
        do_xcom_push=True,
    )

    syndication_get_increment = _bq_get_increment(
        task_id="syndication_get_increment",
        table_id="syndication_increment",
    )

    with TaskGroup(group_id="syndication_run_ind", prefix_group_id=False) as syndication_run_ind:
        syndication_check_new_data = _bq_sql_task(
            task_id="syndication_check_new_data",
            relative_sql_path="syndication/syndication_check_new_data.sql",
            do_xcom_push=True,
        )

        syndication_continue_if_new_data = ShortCircuitOperator(
            task_id="syndication_continue_if_new_data",
            python_callable=continue_if_new_data,
            op_kwargs={
                "check_task_id": "syndication_check_new_data",
            },
            ignore_downstream_trigger_rules=False,
        )

        syndication_check_new_data >> syndication_continue_if_new_data

    with TaskGroup(group_id="syndication_processing", prefix_group_id=False) as syndication_processing:
        syndication_pull_data = _bq_sql_task(
            task_id="syndication_pull_data",
            relative_sql_path="syndication/syndication_pull_data.sql",
        )

        syndication_explode_data = _bq_sql_task(
            task_id="syndication_explode_data",
            relative_sql_path="syndication/syndication_explode_data.sql",
        )

        syndication_pull_data >> syndication_explode_data

    syndication_merge = _bq_sql_task(
        task_id="syndication_merge",
        relative_sql_path="syndication/syndication_merge.sql",
        trigger_rule="none_failed",
    )

    syndication_increment >> syndication_get_increment >> syndication_run_ind >> syndication_processing >> syndication_merge

    # -------------------------------------------------------------------------
    # Final DQ validation
    # -------------------------------------------------------------------------

    validate_ssot_task = PythonOperator(
        task_id="validate_ssot",
        python_callable=validate_ssot,
        op_kwargs={
            "gcp_conn_id": GCP_CONN_ID,
            "bq_location": BQ_LOCATION,
        },
        trigger_rule="none_failed",
    )

    # Cross-pipeline dependencies: sequential merges → final validation.
    events_stream_merge >> ssp_merge >> syndication_merge >> demand_partner_merge >> gam_merge >> validate_ssot_task
