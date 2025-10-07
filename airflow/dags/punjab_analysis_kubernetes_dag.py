"""
Punjab Data Analysis Pipeline - Kubernetes Version
This version uses KubernetesPodOperator for scalable, distributed processing
Simplified to process selected tenants dynamically in single tasks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'punjab-analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    'punjab_analysis_kubernetes_pipeline',
    default_args=default_args,
    description='Punjab property tax analysis using Kubernetes - Scalable and distributed processing',
    schedule_interval=None,  # Manual triggering only
    catchup=False,
    max_active_tasks=10,
    tags=['punjab', 'kubernetes', 'property-tax', 'scalable'],
    params={
        "tenant_ids": ["pb.adampur", "pb.samana", "pb.amloh"],  # Default tenant list
    },
)

# Configuration
DEFAULT_TENANT_IDS = ['pb.adampur', 'pb.samana', 'pb.amloh']
DOCKER_IMAGE = "punjab-analysis:v3.0.0"
NAMESPACE = "punjab-analysis"

# Kubernetes Resource Configuration
def get_container_resources(memory_request="512Mi", memory_limit="2Gi", cpu_request="500m", cpu_limit="2000m"):
    """Get standard resource configuration for pods"""
    return k8s.V1ResourceRequirements(
        requests={"memory": memory_request, "cpu": cpu_request},
        limits={"memory": memory_limit, "cpu": cpu_limit}
    )

# Volume Configuration
def get_volume_mounts():
    """Get standard volume mounts for Punjab analysis pods"""
    return [
        k8s.V1VolumeMount(
            name="data-storage",
            mount_path="/data",
        ),
        k8s.V1VolumeMount(
            name="output-storage",
            mount_path="/output",
        ),
        k8s.V1VolumeMount(
            name="secrets-storage",
            mount_path="/app/secrets",
            read_only=True,
        ),
    ]

def get_volumes():
    """Get standard volumes for Punjab analysis pods"""
    return [
        k8s.V1Volume(
            name="data-storage",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="punjab-data-pvc"
            ),
        ),
        k8s.V1Volume(
            name="output-storage",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="punjab-output-pvc"
            ),
        ),
        k8s.V1Volume(
            name="secrets-storage",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="punjab-secrets-pvc"
            ),
        ),
    ]

def get_tenant_ids(**context):
    """Get tenant IDs from DAG run configuration, params, or use defaults"""
    dag_run = context.get('dag_run')
    tenant_ids = None

    # Try to get from DAG run conf first (command line triggers)
    if dag_run and dag_run.conf:
        # Support both 'tenant_ids' (list) and 'tenant_id' (single string)
        if 'tenant_ids' in dag_run.conf:
            tenant_ids = dag_run.conf.get('tenant_ids')
            logger.info(f"Using tenant IDs from DAG run configuration: {tenant_ids}")
        elif 'tenant_id' in dag_run.conf:
            tenant_ids = [dag_run.conf.get('tenant_id')]
            logger.info(f"Using single tenant ID from DAG run configuration: {tenant_ids}")

    # Try to get from DAG params (UI triggers)
    if tenant_ids is None and 'params' in context:
        if 'tenant_ids' in context['params']:
            tenant_ids = context['params']['tenant_ids']
            logger.info(f"Using tenant IDs from DAG params: {tenant_ids}")
        elif 'tenant_id' in context['params']:
            tenant_ids = [context['params']['tenant_id']]
            logger.info(f"Using single tenant ID from DAG params: {tenant_ids}")

    # Use defaults if nothing provided
    if tenant_ids is None:
        tenant_ids = DEFAULT_TENANT_IDS
        logger.info(f"No configuration provided, using default tenant IDs: {tenant_ids}")

    # Validate tenant IDs format
    if not isinstance(tenant_ids, list):
        raise ValueError(f"tenant_ids must be a list, got: {type(tenant_ids)}")

    # Store tenant IDs in XCom for other tasks to use
    context['ti'].xcom_push(key='tenant_ids', value=tenant_ids)

    logger.info("=" * 60)
    logger.info(f"📋 Selected tenants for processing: {', '.join(tenant_ids)}")
    logger.info("=" * 60)

    return tenant_ids

# Start task - gets and validates tenant configuration
start_task = PythonOperator(
    task_id='start_pipeline',
    python_callable=get_tenant_ids,
    provide_context=True,
    dag=dag,
)

# Data Extraction Task - processes all selected tenants
extract_task = KubernetesPodOperator(
    task_id='extract_data',
    name='extract-all-tenants',
    namespace=NAMESPACE,
    image=DOCKER_IMAGE,
    cmds=["python", "process_all_tenants.py"],
    arguments=[],

    # Environment variables for extraction
    env_vars={
        'MODE': 'extract',
        'TENANT_IDS': '{{ ti.xcom_pull(task_ids="start_pipeline", key="tenant_ids") | tojson }}',
        'OUTPUT_DIR': '/data',
        'EXECUTION_DATE': '{{ ds }}',
        'CONFIG_PATH': '/app/secrets/db_config.yaml',
        'ENVIRONMENT': 'production',
        'DEBUG_MODE': 'true',
    },

    # Resource configuration for extraction (lighter workload)
    container_resources=get_container_resources(
        memory_request="1Gi",
        memory_limit="4Gi",
        cpu_request="1000m",
        cpu_limit="4000m"
    ),

    # Volume mounts
    volume_mounts=get_volume_mounts(),
    volumes=get_volumes(),

    # Kubernetes configuration
    service_account_name="airflow-worker",
    is_delete_operator_pod=True,
    get_logs=True,
    log_events_on_failure=True,

    # Pod security and execution settings
    security_context=k8s.V1SecurityContext(
        run_as_user=1000,
        run_as_group=1000,
        run_as_non_root=True,
    ),

    dag=dag,
)

# Data Analysis Task - processes all selected tenants
analyze_task = KubernetesPodOperator(
    task_id='analyze_data',
    name='analyze-all-tenants',
    namespace=NAMESPACE,
    image=DOCKER_IMAGE,
    cmds=["python", "process_all_tenants.py"],
    arguments=[],

    # Environment variables for analysis
    env_vars={
        'MODE': 'analyze',
        'TENANT_IDS': '{{ ti.xcom_pull(task_ids="start_pipeline", key="tenant_ids") | tojson }}',
        'DATA_DIR': '/data',
        'OUTPUT_DIR': '/output',
        'EXECUTION_DATE': '{{ ds }}',
        'CONFIG_PATH': '/app/secrets/db_config.yaml',
        'ENVIRONMENT': 'production',
        'DEBUG_MODE': 'true',
    },

    # Resource configuration for analysis (heavier workload)
    container_resources=get_container_resources(
        memory_request="2Gi",
        memory_limit="8Gi",
        cpu_request="2000m",
        cpu_limit="8000m"
    ),

    # Volume mounts
    volume_mounts=get_volume_mounts(),
    volumes=get_volumes(),

    # Kubernetes configuration
    service_account_name="airflow-worker",
    is_delete_operator_pod=True,
    get_logs=True,
    log_events_on_failure=True,

    # Pod security and execution settings
    security_context=k8s.V1SecurityContext(
        run_as_user=1000,
        run_as_group=1000,
        run_as_non_root=True,
    ),

    dag=dag,
)

# Completion task
complete_task = PythonOperator(
    task_id='pipeline_complete',
    python_callable=lambda **context: logger.info(
        f"🎉 Punjab Analysis Kubernetes Pipeline completed! "
        f"Execution date: {context['ds']}, Run ID: {context['run_id']}"
    ),
    provide_context=True,
    dag=dag,
)

# Cleanup task using KubernetesPodOperator
cleanup_task = KubernetesPodOperator(
    task_id='cleanup_processed_data',
    name='cleanup-data',
    namespace=NAMESPACE,
    image=DOCKER_IMAGE,
    cmds=["python", "cleanup_data.py"],
    arguments=[],

    # Environment variables for cleanup
    env_vars={
        'DATA_DIR': '/data',
        'TENANT_IDS': '{{ ti.xcom_pull(task_ids="start_pipeline", key="tenant_ids") | tojson }}',
        'CLEANUP_MODE': 'immediate',  # Clean current run data
        'MAX_AGE_HOURS': '0',  # Clean immediately, not after 24 hours
        'DRY_RUN': 'false',
        'CONFIG_PATH': '/app/secrets/db_config.yaml',
        'ENVIRONMENT': 'production',
        'EXECUTION_DATE': '{{ ds }}',
    },

    # Resource configuration for cleanup (light workload)
    container_resources=get_container_resources(
        memory_request="256Mi",
        memory_limit="512Mi",
        cpu_request="100m",
        cpu_limit="500m"
    ),

    # Volume mounts (only need data and secrets)
    volume_mounts=[
        k8s.V1VolumeMount(
            name="data-storage",
            mount_path="/data",
        ),
        k8s.V1VolumeMount(
            name="secrets-storage",
            mount_path="/app/secrets",
            read_only=True,
        ),
    ],
    volumes=[
        k8s.V1Volume(
            name="data-storage",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="punjab-data-pvc"
            ),
        ),
        k8s.V1Volume(
            name="secrets-storage",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name="punjab-secrets-pvc"
            ),
        ),
    ],

    # Kubernetes configuration
    service_account_name="airflow-worker",
    is_delete_operator_pod=True,
    get_logs=True,
    log_events_on_failure=True,

    # Pod security and execution settings
    security_context=k8s.V1SecurityContext(
        run_as_user=1000,
        run_as_group=1000,
        run_as_non_root=True,
    ),

    dag=dag,
)

# Set up linear dependency chain
start_task >> extract_task >> analyze_task >> complete_task >> cleanup_task
