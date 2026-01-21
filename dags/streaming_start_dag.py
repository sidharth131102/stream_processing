from datetime import datetime
import yaml
import tempfile

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
    Load pipeline.yaml from GCS and extract Dataflow-related config.
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
        "parameters": job_cfg["parameters"],
    }


def check_existing_streaming_job(**context):
    """
    Prevent duplicate streaming jobs.
    """
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_pipeline_yaml")

    hook = DataflowHook(
        location=cfg["region"],
    )

    jobs = hook.get_jobs(project_id=cfg["project_id"])

    for job in jobs:
        if (
            job["name"].startswith(cfg["job_name_prefix"])
            and job["currentState"]
            in ("JOB_STATE_RUNNING", "JOB_STATE_DRAINING")
        ):
            return "streaming_job_exists"

    return "start_streaming_job"


# ----------------------------------------------------
# DAG DEFINITION
# ----------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule_interval=None,  # streaming jobs are NOT scheduled
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "streaming"],
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

    streaming_job_exists = EmptyOperator(
        task_id="streaming_job_exists"
    )

    start_streaming_job = DataflowStartFlexTemplateOperator(
        task_id="start_streaming_job",
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
                # IMPORTANT: parameters must be templated as JSON string
                "parameters": (
                    "{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['parameters'] | tojson }}"
                ),
                "environment": {
                    "serviceAccountEmail": (
                        "{{ ti.xcom_pull(task_ids='load_pipeline_yaml')['service_account'] }}"
                    )
                },
            }
        },
    )

    end = EmptyOperator(task_id="end")

    # DAG graph
    start >> load_cfg >> check_job
    check_job >> streaming_job_exists >> end
    check_job >> start_streaming_job >> end
