from datetime import datetime, timedelta
import yaml
import tempfile
import uuid
import logging
import time
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook

# --------------------------------------------------
# DAG METADATA
# --------------------------------------------------
DAG_ID = "dataflow_backfill"
START_DATE = datetime(2024, 1, 1)

# --------------------------------------------------
# Helper: Load configs & validate user input
# --------------------------------------------------
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

# --------------------------------------------------
# Helper: Build Flex Template body
# --------------------------------------------------
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
                "additionalExperiments": ["use_runner_v2"],
            },
        }
    }


# --------------------------------------------------
# Helper: Fetch target table columns
# --------------------------------------------------
# def fetch_target_columns(**context):
#     ti = context["ti"]
#     cfg = ti.xcom_pull(task_ids="load_backfill_config")

#     hook = BigQueryHook(
#         location=Variable.get("bq_location"),          # ✅ REQUIRED for regional datasets
#         use_legacy_sql=False,
#     )

#     sql = f"""
#     SELECT column_name
#     FROM {cfg['project_id']}.{cfg['target_dataset']}.INFORMATION_SCHEMA.COLUMNS
#     WHERE table_name = '{cfg['target_table']}'
#     ORDER BY ordinal_position
#     """

#     rows = hook.get_records(sql)
#     columns = [r[0] for r in rows]

#     if "event_id" not in columns:
#         raise ValueError("event_id must exist in target table")

#     ti.xcom_push(key="columns", value=columns)
# --------------------------------------------------
# Helper: Generate MERGE SQL
# --------------------------------------------------
# def generate_merge_sql(**context):
#     ti = context["ti"]
#     cfg = ti.xcom_pull(task_ids="load_backfill_config")
#     columns = ti.xcom_pull(task_ids="fetch_target_columns", key="columns")

#     immutable = {"event_id", "beam_event_time", "beam_processing_time"}

#     mutable = [c for c in columns if c not in immutable]

#     update_block = ""
#     if mutable:
#         update_block = (
#             "WHEN MATCHED THEN UPDATE SET\n    "
#             + ",\n    ".join(f"{c}=S.{c}" for c in mutable)
#         )

#     insert_cols = ", ".join(columns)
#     insert_vals = ", ".join(f"S.{c}" for c in columns)

#     temp_table = f"{cfg['temp_table_prefix']}_{cfg['run_id']}"

#     sql = f"""
#     MERGE `{cfg['project_id']}.{cfg['target_dataset']}.{cfg['target_table']}` T
#     USING `{cfg['project_id']}.{cfg['temp_dataset']}.{temp_table}` S
#     ON T.event_id = S.event_id
#     {update_block}
#     WHEN NOT MATCHED THEN
#       INSERT ({insert_cols})
#       VALUES ({insert_vals})
#     """

#     ti.xcom_push(key="merge_sql", value=sql)



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



# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "backfill"],
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

    # fetch_columns = PythonOperator(
    #     task_id="fetch_target_columns",
    #     python_callable=fetch_target_columns,
    # )

    # build_merge_sql = PythonOperator(
    #     task_id="generate_merge_sql",
    #     python_callable=generate_merge_sql,
    # )

    # merge_backfill = BigQueryInsertJobOperator(
    #     task_id="merge_backfill_to_main",
    #     location=Variable.get("bq_location"),
    #     configuration={
    #         "query": {
    #             "query": "{{ ti.xcom_pull(task_ids='generate_merge_sql', key='merge_sql') }}",
    #             "useLegacySql": False,
    #         }
    #     },
    # )


    end = EmptyOperator(task_id="end")

    (
        start
        >> load_cfg
        >> start_backfill
        >> wait_for_backfill
        >> end
    )
