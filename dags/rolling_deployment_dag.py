from datetime import datetime
import yaml
import tempfile
import time
import logging

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
    job_cfg = cfg["dataflow"]["job"]

    return {
        "project_id": project_cfg["id"],
        "region": project_cfg["region"],
        "template_path": cfg["dataflow"]["template"]["storage_path"],
        "job_name_prefix": job_cfg["name_prefix"],
        "parameters": {k: str(v) for k, v in job_cfg["parameters"].items()},
        "service_account": (
            f"{cfg['service_account']['name']}"
            f"@{project_cfg['id']}.iam.gserviceaccount.com"
        ),
        "num_workers": job_cfg.get("num_workers", 2),
        "max_workers": job_cfg.get("max_workers", 10),
        "machine_type": job_cfg.get("worker_machine_type", "n2-standard-2"),
        "disk_size_gb": job_cfg.get("disk_size_gb", 30),
        "enable_streaming_engine": job_cfg.get("enable_streaming_engine", True),
    }


def find_existing_job(**context):
    """
    Find an active streaming job to drain.
    """
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_yaml")

    hook = DataflowHook()
    client = hook.get_conn()

    request = client.projects().locations().jobs().list(
        projectId=cfg["project_id"],
        location=cfg["region"],
        filter="ACTIVE",
    )
    response = request.execute()
    jobs = response.get("jobs", [])

    for job in jobs:
        if (
            job.get("name", "").startswith(cfg["job_name_prefix"])
            and job.get("currentState") in ("JOB_STATE_RUNNING", "JOB_STATE_DRAINING")
        ):
            ti.xcom_push(key="job_id", value=job["id"])
            return "stop_existing_job"

    return "start_new_streaming_job"


def wait_for_job_drained(**context):
    """
    Wait (bounded) until the old job is fully drained.
    """
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_yaml")
    job_id = ti.xcom_pull(task_ids="find_existing_job", key="job_id")

    hook = DataflowHook()
    client = hook.get_conn()

    max_wait_seconds = 20 * 60  # 20 minutes
    waited = 0

    while waited < max_wait_seconds:
        job = client.projects().locations().jobs().get(
            projectId=cfg["project_id"],
            location=cfg["region"],
            jobId=job_id,
        ).execute()

        state = job["currentState"]
        logging.info("Old job %s state: %s", job_id, state)

        if state == "JOB_STATE_DRAINED":
            return

        if state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED"):
            raise RuntimeError(f"Old job ended unexpectedly: {state}")

        time.sleep(30)
        waited += 30

    raise TimeoutError("Timed out waiting for job to drain")


def build_flex_body(context, **_):
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_yaml")

    return {
        "launchParameter": {
            "jobName": f"{cfg['job_name_prefix']}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            "containerSpecGcsPath": cfg["template_path"],
            "parameters": cfg["parameters"],
            "environment": {
                "serviceAccountEmail": cfg["service_account"],
                "numWorkers": cfg["num_workers"],
                "maxWorkers": cfg["max_workers"],
                "machineType": cfg["machine_type"],
                "diskSizeGb": cfg["disk_size_gb"],
                "enableStreamingEngine": cfg["enable_streaming_engine"],
                "additionalExperiments": [
                    "use_runner_v2",
                    "enable_vertical_autoscaling",
                ],
            },
        }
    }


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
        drain_pipeline=True,
    )

    wait_for_drain = PythonOperator(
        task_id="wait_for_job_drained",
        python_callable=wait_for_job_drained,
    )

    start_new_streaming_job = DataflowStartFlexTemplateOperator(
        task_id="start_new_streaming_job",
        project_id="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['region'] }}",
        trigger_rule="none_failed_min_one_success",
        body=build_flex_body,
    )

    end = EmptyOperator(task_id="end")

    start >> load_cfg >> find_job
    find_job >> stop_existing_job >> wait_for_drain >> start_new_streaming_job >> end
    find_job >> start_new_streaming_job >> end
