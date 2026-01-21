from datetime import datetime
import yaml
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


# --------------------------------------------------
# DAG CONFIG
# --------------------------------------------------
DAG_ID = "dependency_aware_orchestration"
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

    return {
        "project": cfg["project"]["id"],
        "region": cfg["project"]["region"],
        "job_prefix": cfg["dataflow"]["job"]["name_prefix"],
        "subscription": cfg["dataflow"]["job"]["parameters"]["subscription"],
        "dataset": cfg["bigquery"]["datasets"][0]["name"],
        "table": cfg["bigquery"]["datasets"][0]["tables"][0]["name"],
        "streaming_start_dag": cfg["orchestration"]["dags"]["streaming_start"],
        "recovery_dag": cfg["orchestration"]["dags"]["recovery"],

    }


def check_pubsub(**context):
    cfg = context["ti"].xcom_pull(task_ids="load_pipeline_config")
    hook = PubSubHook()

    project, sub_path = cfg["project"], cfg["subscription"]
    sub_name = sub_path.split("/")[-1]

    sub = hook.get_subscription(project, sub_name)
    return bool(sub)


def check_bigquery(**context):
    cfg = context["ti"].xcom_pull(task_ids="load_pipeline_config")
    hook = BigQueryHook()

    return hook.table_exists(
        project_id=cfg["project"],
        dataset_id=cfg["dataset"],
        table_id=cfg["table"],
    )


def check_dataflow(**context):
    cfg = context["ti"].xcom_pull(task_ids="load_pipeline_config")
    hook = DataflowHook(location=cfg["region"])

    jobs = hook.get_jobs(project_id=cfg["project"])

    for job in jobs:
        if job["name"].startswith(cfg["job_prefix"]):
            return job["currentState"]

    return "NOT_RUNNING"


def decide_next_action(**context):
    ti = context["ti"]

    pubsub_ok = ti.xcom_pull(task_ids="check_pubsub")
    bq_ok = ti.xcom_pull(task_ids="check_bigquery")
    df_state = ti.xcom_pull(task_ids="check_dataflow")

    if not pubsub_ok or not bq_ok:
        return "do_nothing"

    if df_state == "NOT_RUNNING":
        return "trigger_streaming_start"

    if df_state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED"):
        return "trigger_recovery"

    return "do_nothing"


# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule_interval="*/10 * * * *",  # every 10 minutes
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "dependencies"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_cfg = PythonOperator(
        task_id="load_pipeline_config",
        python_callable=load_pipeline_config,
    )

    pubsub = PythonOperator(
        task_id="check_pubsub",
        python_callable=check_pubsub,
    )

    bigquery = PythonOperator(
        task_id="check_bigquery",
        python_callable=check_bigquery,
    )

    dataflow = PythonOperator(
        task_id="check_dataflow",
        python_callable=check_dataflow,
    )

    decide = BranchPythonOperator(
        task_id="decide_next_action",
        python_callable=decide_next_action,
    )

    trigger_streaming = TriggerDagRunOperator(
        task_id="trigger_streaming_start",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='load_pipeline_config')['streaming_start_dag'] }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    trigger_recovery = TriggerDagRunOperator(
        task_id="trigger_recovery",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='load_pipeline_config')['recovery_dag'] }}"    ,
        reset_dag_run=True,
        wait_for_completion=False,
    )

    do_nothing = EmptyOperator(task_id="do_nothing")
    end = EmptyOperator(task_id="end")

    # Graph
    start >> load_cfg
    load_cfg >> [pubsub, bigquery, dataflow]
    [pubsub, bigquery, dataflow] >> decide

    decide >> trigger_streaming >> end
    decide >> trigger_recovery >> end
    decide >> do_nothing >> end
