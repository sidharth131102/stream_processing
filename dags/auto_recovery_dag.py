from datetime import datetime
import yaml
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook


# --------------------------------------------------
# DAG CONFIG
# --------------------------------------------------
DAG_ID = "failure_auto_recovery"
START_DATE = datetime(2024, 1, 1)


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def load_pipeline_config(**context):
    """
    Load pipeline.yaml from GCS and extract recovery-related config.
    """
    bucket = Variable.get("config_bucket")
    path = Variable.get("pipeline_yaml_path")

    hook = GCSHook()
    with tempfile.NamedTemporaryFile() as f:
        hook.download(bucket, path, f.name)
        cfg = yaml.safe_load(f)

    return {
        "project_id": cfg["project"]["id"],
        "region": cfg["project"]["region"],
        "job_prefix": cfg["dataflow"]["job"]["name_prefix"],
        "streaming_start_dag": cfg["orchestration"]["dags"]["streaming_start"],
    }


def check_dataflow_state(**context):
    """
    Determine whether a healthy streaming Dataflow job exists.

    Logic:
    - If at least ONE RUNNING job exists → healthy
    - If only QUEUED / FAILED / CANCELLED jobs exist → recovery needed
    - If no jobs exist → recovery needed
    """
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_config")

    hook = DataflowHook(location=cfg["region"])
    jobs = hook.get_jobs(project_id=cfg["project_id"])

    # Filter jobs belonging to this pipeline
    relevant_jobs = [
        job for job in jobs
        if job["name"].startswith(cfg["job_prefix"])
    ]

    if not relevant_jobs:
        return "NOT_RUNNING"

    # Healthy condition: at least one RUNNING job
    for job in relevant_jobs:
        if job["currentState"] == "JOB_STATE_RUNNING":
            return "RUNNING"

    # No healthy job found
    return "NEEDS_RECOVERY"


def decide_recovery(**context):
    """
    Decide whether to trigger streaming restart.
    """
    state = context["ti"].xcom_pull(task_ids="check_dataflow_state")

    if state in ("NOT_RUNNING", "NEEDS_RECOVERY"):
        return "trigger_streaming_restart"

    return "do_nothing"


# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "recovery", "streaming"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_cfg = PythonOperator(
        task_id="load_pipeline_config",
        python_callable=load_pipeline_config,
    )

    check_state = PythonOperator(
        task_id="check_dataflow_state",
        python_callable=check_dataflow_state,
    )

    decide = BranchPythonOperator(
        task_id="decide_recovery",
        python_callable=decide_recovery,
    )

    trigger_restart = TriggerDagRunOperator(
        task_id="trigger_streaming_restart",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='load_pipeline_config')['streaming_start_dag'] }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    do_nothing = EmptyOperator(task_id="do_nothing")
    end = EmptyOperator(task_id="end")

    # DAG graph
    start >> load_cfg >> check_state >> decide
    decide >> trigger_restart >> end
    decide >> do_nothing >> end
