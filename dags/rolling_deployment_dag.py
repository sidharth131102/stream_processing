from datetime import datetime
import yaml
import tempfile
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
    DataflowStopJobOperator,
)

# ----------------------------------------------------
# DAG METADATA
# ----------------------------------------------------
DAG_ID = "dataflow_streaming_rolling_deploy"
START_DATE = datetime(2024, 1, 1)

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def load_pipeline_yaml(**context):
    bucket = Variable.get("config_bucket")
    path = Variable.get("pipeline_yaml_path")

    hook = GCSHook()
    with tempfile.NamedTemporaryFile() as f:
        hook.download(bucket, path, f.name)
        cfg = yaml.safe_load(f)

    project_cfg = cfg["project"]
    dataflow_cfg = cfg["dataflow"]
    job_cfg = dataflow_cfg["job"]

    return {
        "project_id": project_cfg["id"],
        "region": project_cfg["region"],
        "template_path": dataflow_cfg["template"]["storage_path"],
        "job_name_prefix": job_cfg["name_prefix"],
        "parameters": job_cfg["parameters"],
        "service_account": (
            f"{cfg['service_account']['name']}"
            f"@{project_cfg['id']}.iam.gserviceaccount.com"
        ),
    }


def find_existing_job(**context):
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_yaml")

    hook = DataflowHook(location=cfg["region"])
    jobs = hook.get_jobs(project_id=cfg["project_id"])

    for job in jobs:
        if (
            job["name"].startswith(cfg["job_name_prefix"])
            and job["currentState"]
            in ("JOB_STATE_RUNNING", "JOB_STATE_DRAINING")
        ):
            ti.xcom_push(key="job_id", value=job["id"])
            return "stop_existing_job"

    return "start_new_streaming_job"


def wait_for_job_drained(**context):
    """
    Poll Dataflow until the old streaming job reaches DRAINED.
    """
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_yaml")
    job_id = ti.xcom_pull(task_ids="find_existing_job", key="job_id")

    hook = DataflowHook(location=cfg["region"])

    while True:
        job = hook.get_job(
            project_id=cfg["project_id"],
            job_id=job_id,
        )

        state = job["currentState"]

        if state == "JOB_STATE_DRAINED":
            return

        if state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED"):
            raise RuntimeError(f"Job entered invalid state: {state}")

        time.sleep(30)


# ----------------------------------------------------
# DAG DEFINITION
# ----------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "streaming", "rolling-deploy"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_cfg = PythonOperator(
        task_id="load_pipeline_yaml",
        python_callable=load_pipeline_yaml,
    )

    find_job = BranchPythonOperator(
        task_id="find_existing_job",
        python_callable=find_existing_job,
    )

    stop_existing_job = DataflowStopJobOperator(
        task_id="stop_existing_job",
        project_id="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['region'] }}",
        job_id="{{ ti.xcom_pull(task_ids='find_existing_job', key='job_id') }}",
    )

    wait_for_drain = PythonOperator(
        task_id="wait_for_job_drained",
        python_callable=wait_for_job_drained,
    )

    start_new_streaming_job = DataflowStartFlexTemplateOperator(
        task_id="start_new_streaming_job",
        project_id="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['region'] }}",
        body={
            "launchParameter": {
                "jobName": (
                    "{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['job_name_prefix'] }}"
                    "-{{ ts_nodash }}"
                ),
                "containerSpecGcsPath": (
                    "{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['template_path'] }}"
                ),
                "parameters": (
                    "{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['parameters'] | tojson }}"
                ),
                "environment": {
                    "serviceAccountEmail": (
                        "{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['service_account'] }}"
                    ),
                },
            }
        },
    )

    end = EmptyOperator(task_id="end")

    # DAG graph
    start >> load_cfg >> find_job
    find_job >> stop_existing_job >> wait_for_drain >> start_new_streaming_job >> end
    find_job >> start_new_streaming_job >> end
