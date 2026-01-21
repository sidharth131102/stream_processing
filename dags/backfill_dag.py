from datetime import datetime
import yaml
import tempfile

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
    DataflowStopJobOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor


# --------------------------------------------------
# DAG CONFIG
# --------------------------------------------------
DAG_ID = "backfill_orchestration"
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

    params = context["dag_run"].conf
    start_time = params.get("start_time")
    end_time = params.get("end_time")

    if not start_time or not end_time:
        raise ValueError("start_time and end_time are required for backfill")

    return {
        "project": cfg["project"]["id"],
        "region": cfg["project"]["region"],
        "template_path": cfg["dataflow"]["template"]["storage_path"],
        "job_prefix": cfg["dataflow"]["job"]["name_prefix"],
        "service_account": f"{cfg['service_account']['name']}@{cfg['project']['id']}.iam.gserviceaccount.com",
        "parameters": {
            **cfg["dataflow"]["job"]["parameters"],
            "job_mode": "backfill",
            "start_time": start_time,
            "end_time": end_time,
        },
    }


def validate_backfill_window(**context):
    cfg = context["ti"].xcom_pull(task_ids="load_pipeline_config")

    start = datetime.fromisoformat(cfg["parameters"]["start_time"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(cfg["parameters"]["end_time"].replace("Z", "+00:00"))

    if start >= end:
        raise ValueError("Invalid backfill window: start_time >= end_time")


# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "backfill", "orchestration"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_cfg = PythonOperator(
        task_id="load_pipeline_config",
        python_callable=load_pipeline_config,
    )

    validate_window = PythonOperator(
        task_id="validate_backfill_window",
        python_callable=validate_backfill_window,
    )

    stop_streaming = DataflowStopJobOperator(
        task_id="stop_streaming_job",
        job_name_prefix="{{ ti.xcom_pull(task_ids='load_pipeline_config')['job_prefix'] }}",
        project_id="{{ ti.xcom_pull(task_ids='load_pipeline_config')['project'] }}",
        region="{{ ti.xcom_pull(task_ids='load_pipeline_config')['region'] }}",
        drain=True,
        stop_timeout=1800,
    )

    start_backfill = DataflowStartFlexTemplateOperator(
        task_id="start_backfill_job",
        project_id="{{ ti.xcom_pull(task_ids='load_pipeline_config')['project'] }}",
        location="{{ ti.xcom_pull(task_ids='load_pipeline_config')['region'] }}",
        body={
            "launchParameter": {
                "jobName": "backfill-{{ ts_nodash }}",
                "containerSpecGcsPath": "{{ ti.xcom_pull(task_ids='load_pipeline_config')['template_path'] }}",
                "parameters": {{ ti.xcom_pull(task_ids='load_pipeline_config')['parameters'] | tojson }},
                "environment": {
                    "serviceAccountEmail": "{{ ti.xcom_pull(task_ids='load_pipeline_config')['service_account'] }}"
                },
            }
        },
    )

    wait_for_backfill = DataflowJobStatusSensor(
        task_id="wait_for_backfill_completion",
        project_id="{{ ti.xcom_pull(task_ids='load_pipeline_config')['project'] }}",
        location="{{ ti.xcom_pull(task_ids='load_pipeline_config')['region'] }}",
        job_id="{{ ti.xcom_pull(task_ids='start_backfill_job')['job']['id'] }}",
        expected_statuses={"JOB_STATE_DONE"},
    )

    restart_streaming = TriggerDagRunOperator(
        task_id="restart_streaming_job",
        trigger_dag_id=Variable.get("streaming_start_dag_id"),
        reset_dag_run=True,
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end")

    # Graph
    start >> load_cfg >> validate_window
    validate_window >> stop_streaming
    stop_streaming >> start_backfill >> wait_for_backfill >> restart_streaming >> end
