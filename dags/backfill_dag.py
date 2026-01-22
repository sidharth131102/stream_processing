from datetime import datetime
import yaml
import tempfile

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobStatusSensor,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

# --------------------------------------------------
# DAG METADATA
# --------------------------------------------------
DAG_ID = "dataflow_backfill"
START_DATE = datetime(2024, 1, 1)

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def load_backfill_config(**context):
    dag_conf = context["dag_run"].conf or {}
    start_time = dag_conf.get("start_time")
    end_time = dag_conf.get("end_time")

    if not start_time or not end_time:
        raise ValueError("start_time and end_time are required for backfill")

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

    return {
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

        # IMPORTANT: strings only
        "parameters": {
            **{k: str(v) for k, v in job_cfg["parameters"].items()},
            "job_mode": "backfill",
            "backfill_start_ts": str(start_time),
            "backfill_end_ts": str(end_time),
        },

        "target_dataset": backfill_cfg["sink"]["merge"]["target_dataset"],
        "target_table": backfill_cfg["sink"]["merge"]["target_table"],
        "temp_dataset": backfill_cfg["sink"]["temp_table"]["dataset"],
        "temp_table_prefix": backfill_cfg["sink"]["temp_table"]["table_prefix"],
    }


def build_backfill_body(context, **_):
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_backfill_config")

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
                "stagingLocation": cfg["staging_location"],
                "tempLocation": cfg["temp_location"],
                "additionalExperiments": ["use_runner_v2"],
            },
        }
    }

# --------------------------------------------------
# DAG
# --------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["dataflow", "backfill"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_cfg = PythonOperator(
        task_id="load_backfill_config",
        python_callable=load_backfill_config,
    )

    start_backfill = DataflowStartFlexTemplateOperator(
        task_id="start_backfill_dataflow",
        project_id="{{ ti.xcom_pull('load_backfill_config')['project_id'] }}",
        location="{{ ti.xcom_pull('load_backfill_config')['region'] }}",
        body=build_backfill_body,
    )

    wait_for_backfill = DataflowJobStatusSensor(
        task_id="wait_for_backfill_completion",
        project_id="{{ ti.xcom_pull('load_backfill_config')['project_id'] }}",
        location="{{ ti.xcom_pull('load_backfill_config')['region'] }}",
        job_id="{{ ti.xcom_pull(task_ids='start_backfill_dataflow')['job_id'] }}",
        expected_statuses={"JOB_STATE_DONE"},
        poke_interval=60,
        timeout=60 * 60 * 6,  # 6 hours
    )

    merge_backfill = BigQueryInsertJobOperator(
        task_id="merge_backfill_to_main",
        configuration={
            "query": {
                "query": """
                -- PUT YOUR MERGE SQL HERE
                """,
                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(task_id="end")

    start >> load_cfg >> start_backfill >> wait_for_backfill >> merge_backfill >> end
