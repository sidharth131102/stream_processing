from datetime import datetime,timedelta
import yaml
import tempfile

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
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
# Helper: Load configs
# --------------------------------------------------
def build_gcs_path_pattern(
    bucket: str,
    prefix: str,
    start_time: str,
    end_time: str,
) -> str:
    start = datetime.fromisoformat(start_time)
    end = datetime.fromisoformat(end_time)

    patterns = []
    current = start

    while current < end:
        patterns.append(
            f"gs://{bucket}/{prefix}/"
            f"{current:%Y}/{current:%m}/{current:%d}/*.json"
        )
        current += timedelta(days=1)

    return ",".join(patterns)

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

    base_parameters = {
        k: str(v)
        for k, v in job_cfg["parameters"].items()
        if k not in ("subscription", "job_mode")
    }
    path_pattern = build_gcs_path_pattern(
    bucket=backfill_cfg["source"]["archive_bucket"],
    prefix=backfill_cfg["source"]["prefix"],
    start_time=start_time,
    end_time=end_time,
    )


    safety_cfg = backfill_cfg.get("safety", {})

    timeout_seconds = int(safety_cfg.get("dataflow_timeout_seconds", 6 * 60 * 60))

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

        "parameters": {
            **base_parameters,
            "job_mode": "backfill",
            "backfill_start_ts": start_time,
            "backfill_end_ts": end_time,
            "path_pattern": path_pattern,
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
def fetch_target_columns(**context):
    cfg = context["ti"].xcom_pull(task_ids="load_backfill_config")

    hook = BigQueryHook()
    sql = f"""
    SELECT column_name
    FROM `{cfg['project_id']}.{cfg['target_dataset']}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{cfg['target_table']}'
    ORDER BY ordinal_position
    """

    rows = hook.get_records(sql)
    columns = [r[0] for r in rows]

    if "event_id" not in columns:
        raise ValueError("event_id must exist in target table")

    context["ti"].xcom_push(key="columns", value=columns)

def enforce_timeout(**context):
    cfg = context["ti"].xcom_pull(task_ids="load_backfill_config")
    timeout = cfg["dataflow_timeout_seconds"]

    if timeout <= 0:
        raise ValueError("Invalid dataflow_timeout_seconds")
    

# --------------------------------------------------
# Helper: Generate MERGE SQL dynamically
# --------------------------------------------------
def generate_merge_sql(**context):
    ti = context["ti"]
    cfg = ti.xcom_pull(task_ids="load_backfill_config")
    columns = ti.xcom_pull(task_ids="fetch_target_columns", key="columns")

    merge_keys = ["event_id"]
    non_key_cols = [c for c in columns if c not in merge_keys]

    update_clause = ",\n      ".join(
        f"{c} = S.{c}" for c in non_key_cols
    )

    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"S.{c}" for c in columns)

    temp_table = f"{cfg['temp_table_prefix']}_{cfg['run_id']}"

    sql = f"""
    MERGE `{cfg['project_id']}.{cfg['target_dataset']}.{cfg['target_table']}` T
    USING `{cfg['project_id']}.{cfg['temp_dataset']}.{temp_table}` S
    ON T.event_id = S.event_id

    WHEN MATCHED THEN
      UPDATE SET
        {update_clause}

    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """

    ti.xcom_push(key="merge_sql", value=sql)


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
) as dag:

    start = EmptyOperator(task_id="start")

    load_cfg = PythonOperator(
        task_id="load_backfill_config",
        python_callable=load_backfill_config,
    )
    
    validate_timeout = PythonOperator(
    task_id="validate_backfill_timeout",
    python_callable=enforce_timeout,
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
        job_id="{{ ti.xcom_pull('start_backfill_dataflow')['id'] }}",
        expected_statuses={"JOB_STATE_DONE"},
        poke_interval=60,

        # ✅ THIS *CAN* BE DYNAMIC
        execution_timeout=timedelta(hours=24),
        
    )
    fetch_columns = PythonOperator(
        task_id="fetch_target_columns",
        python_callable=fetch_target_columns,
    )

    build_merge_sql = PythonOperator(
        task_id="generate_merge_sql",
        python_callable=generate_merge_sql,
    )

    merge_backfill = BigQueryInsertJobOperator(
        task_id="merge_backfill_to_main",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='generate_merge_sql', key='merge_sql') }}",
                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(task_id="end")

    # Graph
    (
        start
        >> load_cfg
        >> validate_timeout
        >> start_backfill
        >> wait_for_backfill
        >> fetch_columns
        >> build_merge_sql
        >> merge_backfill
        >> end
    )
