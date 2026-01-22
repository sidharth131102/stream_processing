from datetime import datetime
import yaml
import tempfile
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStopJobOperator,
)

# ----------------------------------------------------
# DAG METADATA
# ----------------------------------------------------
DAG_ID = "dataflow_streaming_stop"
START_DATE = datetime(2024, 1, 1)

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def load_pipeline_yaml(**context):
    """
    Load pipeline.yaml from GCS and extract Dataflow job identity.
    """
    bucket = Variable.get("config_bucket")
    path = Variable.get("pipeline_yaml_path")

    hook = GCSHook()
    with tempfile.NamedTemporaryFile() as f:
        hook.download(bucket, path, f.name)
        cfg = yaml.safe_load(f)

    project_cfg = cfg["project"]
    job_cfg = cfg["dataflow"]["job"]

    return {
        "project_id": project_cfg["id"],
        "region": project_cfg["region"],
        "job_name_prefix": job_cfg["name_prefix"],
    }


def find_running_streaming_job(**context):
    """
    Find a RUNNING or DRAINING streaming job.
    Fail CLOSED: if API fails, do nothing.
    """
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_yaml")

    hook = DataflowHook()
    client = hook.get_conn()

    try:
        request = client.projects().locations().jobs().list(
            projectId=cfg["project_id"],
            location=cfg["region"],
            filter="ACTIVE",
        )
        response = request.execute()
        jobs = response.get("jobs", [])
    except Exception as e:
        logging.error(
            "Failed to list Dataflow jobs, skipping stop. Error: %s",
            e,
        )
        return "no_streaming_job_found"

    for job in jobs:
        if (
            job.get("name", "").startswith(cfg["job_name_prefix"])
            and job.get("currentState")
            in ("JOB_STATE_RUNNING", "JOB_STATE_DRAINING")
        ):
            ti.xcom_push(key="job_id", value=job["id"])
            return "stop_streaming_job"

    return "no_streaming_job_found"


# ----------------------------------------------------
# DAG DEFINITION
# ----------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule_interval=None,  # manual only
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "streaming", "stop"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_cfg = PythonOperator(
        task_id="load_pipeline_yaml",
        python_callable=load_pipeline_yaml,
    )

    find_job = BranchPythonOperator(
        task_id="find_running_streaming_job",
        python_callable=find_running_streaming_job,
    )

    no_streaming_job_found = EmptyOperator(
        task_id="no_streaming_job_found"
    )

    stop_streaming_job = DataflowStopJobOperator(
        task_id="stop_streaming_job",
        project_id="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['region'] }}",
        job_id="{{ ti.xcom_pull(task_ids='find_running_streaming_job', key='job_id') }}",
        drain_pipeline=True,  # ✅ CRITICAL for streaming correctness
    )

    end = EmptyOperator(task_id="end")

    # DAG graph
    start >> load_cfg >> find_job
    find_job >> no_streaming_job_found >> end
    find_job >> stop_streaming_job >> end
