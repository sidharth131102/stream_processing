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
    DataflowStartFlexTemplateOperator,
)

# ----------------------------------------------------
# DAG METADATA
# ----------------------------------------------------
DAG_ID = "dataflow_streaming_start"
START_DATE = datetime(2024, 1, 1)

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def load_pipeline_yaml(**context):
    """
    Load pipeline.yaml from GCS and extract Dataflow configuration.
    Includes staging and temp locations to prevent 404 job_object errors.
    """
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
        "service_account": (
            f"{cfg['service_account']['name']}"
            f"@{project_cfg['id']}.iam.gserviceaccount.com"
        ),

        # Parameters must be dict[str, str] for the API
        "parameters": {
            k: str(v) for k, v in job_cfg["parameters"].items()
        },

        # Environment / worker config
        "num_workers": int(job_cfg.get("num_workers", 2)),
        "max_workers": int(job_cfg.get("max_workers", 10)),
        "machine_type": job_cfg.get("worker_machine_type", "n2-standard-2"),
        "disk_size_gb": int(job_cfg.get("disk_size_gb", 30)),
        "enable_streaming_engine": bool(
            job_cfg.get("enable_streaming_engine", True)
        ),
        # CRITICAL: Explicit staging for Flex Template
        "staging_location": job_cfg["staging_location"],
        "temp_location": job_cfg["temp_location"],
    }


def check_existing_streaming_job(**context):
    """
    Check for active streaming jobs to avoid duplicate execution.
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
        logging.error("Unable to list Dataflow jobs: %s", e)
        return "streaming_job_exists"

    for job in jobs:
        if (
            job.get("name", "").startswith(cfg["job_name_prefix"])
            and job.get("currentState") in ("JOB_STATE_RUNNING", "JOB_STATE_STARTING")
        ):
            logging.info("Found active job %s", job["name"])
            return "streaming_job_exists"

    return "start_streaming_job"


def build_flex_template_body(context, **_):
    """
    Constructs the API body for the Flex Template launch.
    Ensures stagingLocation is explicitly set to avoid 404 job_object errors.
    """
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_yaml")

    return {
        "launchParameter": {
            "jobName": (
                f"{cfg['job_name_prefix']}-"
                f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            ),
            "containerSpecGcsPath": cfg["template_path"],
            "parameters": cfg["parameters"],
            "environment": {
                "serviceAccountEmail": cfg["service_account"],
                "numWorkers": cfg["num_workers"],
                "maxWorkers": cfg["max_workers"],
                "machineType": cfg["machine_type"],
                "diskSizeGb": cfg["disk_size_gb"],
                "enableStreamingEngine": cfg["enable_streaming_engine"],
                # FIX: Explicitly set staging paths in the environment block
                "stagingLocation": cfg["staging_location"],
                "tempLocation": cfg["temp_location"],
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
    tags=["dataflow", "streaming", "optimized"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_cfg = PythonOperator(
        task_id="load_pipeline_yaml",
        python_callable=load_pipeline_yaml,
    )

    check_job = BranchPythonOperator(
        task_id="check_existing_streaming_job",
        python_callable=check_existing_streaming_job,
    )

    streaming_job_exists = EmptyOperator(task_id="streaming_job_exists")

    start_streaming_job = DataflowStartFlexTemplateOperator(
        task_id="start_streaming_job",
        project_id="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['project_id'] }}",
        location="{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['region'] }}",
        body=build_flex_template_body,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # Execution Flow
    start >> load_cfg >> check_job
    check_job >> streaming_job_exists >> end
    check_job >> start_streaming_job >> end