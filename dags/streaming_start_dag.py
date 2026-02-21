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
    template_cfg = dataflow_cfg["template"]
    network_cfg = cfg.get("network", {})
    subnet_cfg = network_cfg.get("subnetwork", {})

    return {
        "project_id": project_cfg["id"],
        "region": project_cfg["region"],
        "template_path": template_cfg["storage_path"],
        "safe_template_path": template_cfg.get("safe_storage_path"),
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
        "experiments": job_cfg.get("experiments", []),
        # CRITICAL: Explicit staging for Flex Template
        "staging_location": job_cfg["staging_location"],
        "temp_location": job_cfg["temp_location"],
        "security": cfg.get("security", {}),
        "network": {
            "enabled": bool(network_cfg.get("enabled", False)),
            "network": network_cfg.get("name"),
            "subnetwork": subnet_cfg.get("self_link"),
            "subnetwork_name": subnet_cfg.get("name"),
            "subnetwork_region": subnet_cfg.get("region", project_cfg["region"]),
            "use_public_ips": bool(
                network_cfg.get("dataflow", {}).get("use_public_ips", False)
            ),
        },
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
    dag_conf = (context.get("dag_run").conf or {})

    if dag_conf.get("template_path"):
        template_path = dag_conf["template_path"]
    elif dag_conf.get("template_mode") == "safe" and cfg.get("safe_template_path"):
        template_path = cfg["safe_template_path"]
    else:
        template_path = cfg["template_path"]

    logging.info(
        "Launching streaming job with template_path=%s (mode=%s)",
        template_path,
        dag_conf.get("template_mode", "latest"),
    )

    # -------------------------------
    # Base environment (existing)
    # -------------------------------
    environment = {
        "serviceAccountEmail": cfg["service_account"],
        "numWorkers": cfg["num_workers"],
        "maxWorkers": cfg["max_workers"],
        "machineType": cfg["machine_type"],
        "diskSizeGb": cfg["disk_size_gb"],
        "enableStreamingEngine": cfg["enable_streaming_engine"],
        "stagingLocation": cfg["staging_location"],
        "tempLocation": cfg["temp_location"],
        "additionalExperiments": cfg.get("experiments", []),
    }

    # -------------------------------------------------
    # ✅ ADD THIS BLOCK: CMEK support (config-driven)
    # -------------------------------------------------
    security_cfg = cfg.get("security", {}).get("cmek", {})

    if security_cfg.get("enabled"):
        environment["kmsKeyName"] = security_cfg["key_name"]
        logging.info(
            "Launching Dataflow job with CMEK: %s",
            security_cfg["key_name"],
        )

    network_cfg = cfg.get("network", {})
    if network_cfg.get("enabled"):
        if network_cfg.get("network"):
            environment["network"] = network_cfg["network"]

        if network_cfg.get("subnetwork"):
            environment["subnetwork"] = network_cfg["subnetwork"]
        elif network_cfg.get("subnetwork_name"):
            environment["subnetwork"] = (
                f"regions/{network_cfg['subnetwork_region']}/subnetworks/"
                f"{network_cfg['subnetwork_name']}"
            )

        environment["ipConfiguration"] = (
            "WORKER_IP_PUBLIC"
            if network_cfg.get("use_public_ips")
            else "WORKER_IP_PRIVATE"
        )

    # Runtime overrides when triggering the DAG manually.
    if dag_conf.get("network"):
        environment["network"] = dag_conf["network"]
    if dag_conf.get("subnetwork"):
        environment["subnetwork"] = dag_conf["subnetwork"]
    if dag_conf.get("use_public_ips") is not None:
        environment["ipConfiguration"] = (
            "WORKER_IP_PUBLIC"
            if bool(dag_conf["use_public_ips"])
            else "WORKER_IP_PRIVATE"
        )

    # -------------------------------
    # Final request body
    # -------------------------------
    return {
        "launchParameter": {
            "jobName": (
                f"{cfg['job_name_prefix']}-"
                f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            ),
            "containerSpecGcsPath": template_path,
            "parameters": cfg["parameters"],
            "environment": environment,
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
