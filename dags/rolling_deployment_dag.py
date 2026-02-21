from datetime import datetime
import yaml
import tempfile
import time
import logging
import uuid
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
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


def load_backfill_config(**context):
    dag_conf = context["dag_run"].conf or {}

    start_time = dag_conf.get("start_time")
    end_time = dag_conf.get("end_time")

    if not start_time or not end_time:
        raise ValueError("start_time and end_time are required (ISO-8601)")

    # Validate ISO-8601 early
    datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    datetime.fromisoformat(end_time.replace("Z", "+00:00"))

    run_id = uuid.uuid4().hex[:12]

    config_bucket = Variable.get("config_bucket")
    pipeline_yaml_path = Variable.get("pipeline_yaml_path")

    hook = GCSHook()

    with tempfile.NamedTemporaryFile() as f:
        hook.download(config_bucket, pipeline_yaml_path, f.name)
        pipeline_cfg = yaml.safe_load(f)

    with tempfile.NamedTemporaryFile() as f:
        hook.download(config_bucket, "backfill.yaml", f.name)
        backfill_cfg = yaml.safe_load(f)

    job_cfg = pipeline_cfg["dataflow"]["job"]
    backfill_job_cfg = backfill_cfg["dataflow"]["job"]

    base_parameters = {
        k: str(v)
        for k, v in job_cfg["parameters"].items()
        if k not in ("subscription", "job_mode")
    }
    base_parameters["subscription"] = "projects/dummy/subscriptions/dummy"
    safety_cfg = backfill_cfg.get("safety", {})
    timeout_seconds = int(safety_cfg.get("dataflow_timeout_seconds", 6 * 60 * 60))

    return {
        "run_id": run_id,
        "project_id": pipeline_cfg["project"]["id"],
        "region": pipeline_cfg["project"]["region"],
        "template_path": pipeline_cfg["dataflow"]["template"]["storage_path"],

        "job_name_prefix": backfill_job_cfg["name_prefix"],
        "service_account": (
            f"{pipeline_cfg['service_account']['name']}"
            f"@{pipeline_cfg['project']['id']}.iam.gserviceaccount.com"
        ),

        "staging_location": job_cfg["staging_location"],
        "temp_location": job_cfg["temp_location"],

        "num_workers": int(backfill_job_cfg.get("num_workers", 2)),
        "max_workers": int(backfill_job_cfg.get("max_workers", 4)),
        "machine_type": backfill_job_cfg.get("worker_machine_type", "e2-standard-4"),
        "experiments": job_cfg.get("experiments", []),
        "parameters": {
            **base_parameters,
            "job_mode": "backfill",
            "backfill_start_ts": start_time,
            "backfill_end_ts": end_time,
        },

        "dataflow_timeout_seconds": timeout_seconds,

        "target_dataset": backfill_cfg["sink"]["merge"]["target_dataset"],
        "target_table": backfill_cfg["sink"]["merge"]["target_table"],
        "temp_dataset": backfill_cfg["sink"]["temp_table"]["dataset"],
        "temp_table_prefix": backfill_cfg["sink"]["temp_table"]["table_prefix"],
    }

def build_backfill_body(context, **_):
    cfg = context["ti"].xcom_pull(task_ids="load_backfill_config")

    return {
        "launchParameter": {
            "jobName": f"{cfg['job_name_prefix']}-{cfg['run_id']}",
            "containerSpecGcsPath": cfg["template_path"],
            "parameters": cfg["parameters"],
            "environment": {
                "serviceAccountEmail": cfg["service_account"],
                "numWorkers": cfg["num_workers"],
                "maxWorkers": cfg["max_workers"],
                "machineType": cfg["machine_type"],
                "stagingLocation": cfg["staging_location"],
                "tempLocation": cfg["temp_location"],
                "additionalExperiments": cfg.get("experiments", []),
            },
        }
    }

def wait_for_dataflow_job(**context):
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_backfill_config")

    job_id = ti.xcom_pull(task_ids="start_backfill_dataflow")["id"]

    hook = DataflowHook()
    client = hook.get_conn()

    while True:
        job = (
            client.projects()
            .locations()
            .jobs()
            .get(
                projectId=cfg["project_id"],
                location=cfg["region"],
                jobId=job_id,
            )
            .execute()
        )

        state = job.get("currentState")
        logging.info("Dataflow job %s state: %s", job_id, state)

        if state == "JOB_STATE_DONE":
            return

        if state in (
            "JOB_STATE_FAILED",
            "JOB_STATE_CANCELLED",
            "JOB_STATE_DRAINED",
        ):
            raise RuntimeError(f"Dataflow job ended in state {state}")

        time.sleep(60)

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
    tags=["dataflow", "streaming", "backfill","rolling-deploy"],
    params={
        "start_time": Param(
            "",
            type="string",
            description="Backfill start time (ISO-8601, e.g. 2024-01-01T00:00:00Z)",
        ),
        "end_time": Param(
            "",
            type="string",
            description="Backfill end time (ISO-8601, e.g. 2024-01-02T00:00:00Z)",
        ),
    },
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
    load_backfill_cfg = PythonOperator(
        task_id="load_backfill_config",
        python_callable=load_backfill_config,
    )
    start_backfill = DataflowStartFlexTemplateOperator(
        task_id="start_backfill_dataflow",
        project_id="{{ ti.xcom_pull('load_backfill_config')['project_id'] }}",
        location="{{ ti.xcom_pull('load_backfill_config')['region'] }}",
        body=build_backfill_body,
    )

    wait_for_backfill = PythonOperator(
        task_id="wait_for_backfill_completion",
        python_callable=wait_for_dataflow_job,
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
    find_job >> stop_existing_job >> wait_for_drain >> load_backfill_cfg >> start_backfill >> wait_for_backfill >> start_new_streaming_job >> end
    find_job >> load_backfill_cfg >> start_backfill >> wait_for_backfill >> start_new_streaming_job >> end