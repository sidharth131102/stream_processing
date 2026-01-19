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
    bucket = Variable.get("config_bucket")
    path = Variable.get("pipeline_yaml_path")

    hook = GCSHook()
    with tempfile.NamedTemporaryFile() as f:
        hook.download(bucket, path, f.name)
        cfg = yaml.safe_load(f)

    return {
        "project": cfg["project"]["id"],
        "region": cfg["project"]["region"],
        "job_prefix": cfg["dataflow"]["job"]["name_prefix"],
        "streaming_start_dag": cfg["orchestration"]["dags"]["streaming_start"]
    }


def check_dataflow_state(**context):
    cfg = context["ti"].xcom_pull(task_ids="load_pipeline_config")
    hook = DataflowHook(location=cfg["region"])

    jobs = hook.get_jobs(project_id=cfg["project"])

    for job in jobs:
        if job["name"].startswith(cfg["job_prefix"]):
            return job["currentState"]

    return "NOT_RUNNING"


def decide_recovery(**context):
    state = context["ti"].xcom_pull(task_ids="check_dataflow_state")

    if state in (
        "JOB_STATE_FAILED",
        "JOB_STATE_CANCELLED",
        "NOT_RUNNING",
    ):
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

    restart_streaming = TriggerDagRunOperator(
        task_id="trigger_streaming_restart",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='load_pipeline_config')['streaming_start_dag'] }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    do_nothing = EmptyOperator(task_id="do_nothing")
    end = EmptyOperator(task_id="end")

    # Graph
    start >> load_cfg >> check_state >> decide
    decide >> restart_streaming >> end
    decide >> do_nothing >> end
