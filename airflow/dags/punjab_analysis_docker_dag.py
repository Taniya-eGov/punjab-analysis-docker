"""
Punjab Data Analysis Pipeline - Docker Version
This version uses DockerOperator instead of KubernetesPodOperator to avoid networking issues
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
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
    'punjab_analysis_docker_pipeline',
    default_args=default_args,
    description='Punjab property tax analysis using Docker - Parameterized for flexible tenant processing',
    schedule_interval=None,  # Manual triggering only - no automatic schedule
    catchup=False,
    max_active_tasks=5,
    tags=['punjab', 'docker', 'property-tax', 'parameterized'],
    params={
        "tenant_ids": ["pb.adampur", "pb.samana", "pb.amloh"],  # Default tenant list - can be overridden
    },
)

# Configuration
DEFAULT_TENANT_IDS = ['pb.adampur', 'pb.samana', 'pb.amloh']  # Fallback if no params provided
DOCKER_IMAGE = "punjab-analysis:v3.0.0"

def should_process_tenant(tenant_id, **context):
    """Check if tenant should be processed based on DAG configuration"""
    tenant_ids = context['ti'].xcom_pull(task_ids='start_pipeline', key='tenant_ids')
    should_run = tenant_id in tenant_ids
    logger.info(f"Tenant {tenant_id}: {'PROCESSING' if should_run else 'SKIPPING'}")
    return should_run

def create_tenant_pipeline(tenant_id):
    """Create a complete pipeline for a single tenant using Docker"""

    with TaskGroup(group_id=f'process_{tenant_id.replace(".", "_")}', dag=dag) as tg:

        # Conditional check - skip entire tenant if not in configuration
        tenant_check = ShortCircuitOperator(
            task_id='check_tenant_enabled',
            python_callable=should_process_tenant,
            op_args=[tenant_id],
            provide_context=True,
            dag=dag,
        )

        # Data Extraction Task
        extract_task = DockerOperator(
            task_id='extract_data',
            image=DOCKER_IMAGE,
            api_version='auto',
            auto_remove=True,
            command="python main.py",
            docker_url='unix://var/run/docker.sock',
            network_mode='host',
            mount_tmp_dir=False,

            # Environment variables - only workflow-specific, no DB credentials
            environment={
                'MODE': 'extract',
                'TENANT_ID': tenant_id,
                'OUTPUT_DIR': '/data',
                'EXECUTION_DATE': '{{ ds }}',
                'CONFIG_PATH': '/run/secrets/db_config.yaml',
                'ENVIRONMENT': 'development',
                'DEBUG_MODE': 'true',
            },

            # Volume mounts - data directory and secrets
            mounts=[
                {
                    'source': '/home/admin1/Desktop/punjab-analysis-fresh/data',
                    'target': '/data',
                    'type': 'bind'
                },
                {
                    'source': '/home/admin1/Desktop/punjab-analysis-fresh/secrets',
                    'target': '/run/secrets',
                    'type': 'bind',
                    'read_only': True
                }
            ],

            dag=dag,
        )

        # Data Analysis Task
        analyze_task = DockerOperator(
            task_id='analyze_data',
            image=DOCKER_IMAGE,
            api_version='auto',
            auto_remove=True,
            command="python main.py",
            docker_url='unix://var/run/docker.sock',
            network_mode='host',
            mount_tmp_dir=False,

            # Environment variables - only workflow-specific, no DB credentials
            environment={
                'MODE': 'analyze',
                'TENANT_ID': tenant_id,
                'DATA_DIR': '/data',
                'OUTPUT_DIR': '/output',
                'EXECUTION_DATE': '{{ ds }}',
                'CONFIG_PATH': '/run/secrets/db_config.yaml',
                'ENVIRONMENT': 'development',
                'DEBUG_MODE': 'true',
            },

            # Volume mounts - data, output directories and secrets
            mounts=[
                {
                    'source': '/home/admin1/Desktop/punjab-analysis-fresh/data',
                    'target': '/data',
                    'type': 'bind'
                },
                {
                    'source': '/home/admin1/Desktop/punjab-analysis-fresh/local-reports',
                    'target': '/output',
                    'type': 'bind'
                },
                {
                    'source': '/home/admin1/Desktop/punjab-analysis-fresh/secrets',
                    'target': '/run/secrets',
                    'type': 'bind',
                    'read_only': True
                }
            ],

            dag=dag,
        )

        # Set task dependencies
        tenant_check >> extract_task >> analyze_task

        return tg

def get_tenant_ids(**context):
    """Get tenant IDs from DAG run configuration, params, or use defaults"""
    dag_run = context.get('dag_run')

    # Try to get from DAG run conf first (command line triggers)
    if dag_run and dag_run.conf and 'tenant_ids' in dag_run.conf:
        tenant_ids = dag_run.conf.get('tenant_ids')
        logger.info(f"Using tenant IDs from DAG run configuration: {tenant_ids}")
    # Try to get from DAG params (UI triggers)
    elif 'params' in context and 'tenant_ids' in context['params']:
        tenant_ids = context['params']['tenant_ids']
        logger.info(f"Using tenant IDs from DAG params: {tenant_ids}")
    else:
        tenant_ids = DEFAULT_TENANT_IDS
        logger.info(f"No configuration provided, using default tenant IDs: {tenant_ids}")

    # Validate tenant IDs format
    if not isinstance(tenant_ids, list):
        raise ValueError(f"tenant_ids must be a list, got: {type(tenant_ids)}")

    # Store tenant IDs in XCom for other tasks to use
    context['ti'].xcom_push(key='tenant_ids', value=tenant_ids)
    return tenant_ids

# Start task - now gets and validates tenant configuration
start_task = PythonOperator(
    task_id='start_pipeline',
    python_callable=get_tenant_ids,
    provide_context=True,
    dag=dag,
)

# Simple approach: Process tenants with detailed logging but clean UI
def process_tenants_with_detailed_logs(**context):
    """Process selected tenants with step-by-step logging for visibility"""
    import psycopg2
    import subprocess

    tenant_ids = context['ti'].xcom_pull(task_ids='start_pipeline', key='tenant_ids')

    # Database connection - try to connect to Punjab data
    # The Punjab data might be in airflowdb database with postgres/postgres credentials
    db_config = {
        'host': 'postgres',
        'database': 'airflowdb',  # Punjab data database
        'user': 'postgres',       # Punjab data user
        'password': 'postgres',   # Punjab data password
        'port': '5432'
    }

    logger.info(f"🚀 STARTING PIPELINE FOR TENANTS: {tenant_ids}")
    logger.info("=" * 60)

    processed = []
    failed = []

    for i, tenant_id in enumerate(tenant_ids, 1):
        logger.info(f"📋 TENANT {i}/{len(tenant_ids)}: {tenant_id.upper()}")
        logger.info("-" * 40)

        try:
            # Skip database validation - let extraction handle connectivity
            # The extraction containers use host networking and can connect properly
            logger.info(f"🔍 STEP 1: Starting processing for {tenant_id} (skipping pre-validation)")

            # Extraction
            logger.info(f"📥 STEP 2: Extracting data for {tenant_id}")
            extract_cmd = [
                'docker', 'run', '--rm',
                '--network', 'host',
                '-v', '/home/admin1/Desktop/punjab-analysis-fresh/data:/data',
                '-v', '/home/admin1/Desktop/punjab-analysis-fresh/secrets:/run/secrets:ro',
                '-e', 'MODE=extract',
                '-e', f'TENANT_ID={tenant_id}',
                '-e', 'OUTPUT_DIR=/data',
                '-e', 'CONFIG_PATH=/run/secrets/db_config.yaml',
                '-e', 'ENVIRONMENT=development',
                '-e', 'DEBUG_MODE=true',
                DOCKER_IMAGE,
                'python', 'main.py'
            ]

            result = subprocess.run(extract_cmd, capture_output=True, text=True, timeout=3600)
            if result.returncode != 0:
                raise Exception(f"Extraction failed: {result.stderr}")

            # Log detailed extraction output
            logger.info(f"📋 {tenant_id}: EXTRACTION OUTPUT:")
            output_lines = (result.stdout + result.stderr).split('\n')
            for line in output_lines:
                if line.strip() and '- INFO -' in line and any(keyword in line.lower() for keyword in ['saved', 'total records', 'chunk', 'records →']):
                    # Extract just the message part after the timestamp and level
                    if 'records to' in line:
                        parts = line.split('- INFO - ')
                        if len(parts) > 1:
                            logger.info(f"   📄 {parts[1]}")
                    elif 'total records' in line.lower():
                        parts = line.split('- INFO - ')
                        if len(parts) > 1:
                            logger.info(f"   📊 {parts[1]}")
                    elif 'chunk' in line.lower() and 'records →' in line:
                        parts = line.split('- INFO - ')
                        if len(parts) > 1:
                            logger.info(f"   🔄 {parts[1]}")

            logger.info(f"✅ {tenant_id}: Data extraction completed successfully")

            # Analysis
            logger.info(f"📊 STEP 3: Analyzing data for {tenant_id}")
            analyze_cmd = [
                'docker', 'run', '--rm',
                '--network', 'host',
                '-v', '/home/admin1/Desktop/punjab-analysis-fresh/data:/data',
                '-v', '/home/admin1/Desktop/punjab-analysis-fresh/local-reports:/output',
                '-v', '/home/admin1/Desktop/punjab-analysis-fresh/secrets:/run/secrets:ro',
                '-e', 'MODE=analyze',
                '-e', f'TENANT_ID={tenant_id}',
                '-e', 'DATA_DIR=/data',
                '-e', 'OUTPUT_DIR=/output',
                '-e', 'CONFIG_PATH=/run/secrets/db_config.yaml',
                '-e', 'ENVIRONMENT=development',
                '-e', 'DEBUG_MODE=true',
                DOCKER_IMAGE,
                'python', 'main.py'
            ]

            result = subprocess.run(analyze_cmd, capture_output=True, text=True, timeout=3600)
            if result.returncode != 0:
                raise Exception(f"Analysis failed: {result.stderr}")

            # Log detailed analysis output
            logger.info(f"📊 {tenant_id}: ANALYSIS OUTPUT:")
            output_lines = (result.stdout + result.stderr).split('\n')
            for line in output_lines:
                if line.strip() and '- INFO -' in line and any(keyword in line.lower() for keyword in ['loaded', 'active', 'combined', 'analysis completed', 'report saved', 'generated']):
                    # Extract just the message part after the timestamp and level
                    parts = line.split('- INFO - ')
                    if len(parts) > 1:
                        message = parts[1]
                        if 'loaded' in message.lower() and ('property' in message.lower() or 'owner' in message.lower() or 'unit' in message.lower()):
                            logger.info(f"   📄 {message}")
                        elif 'combined' in message.lower():
                            logger.info(f"   🔄 {message}")
                        elif 'analysis completed' in message.lower() or 'report saved' in message.lower():
                            logger.info(f"   ✅ {message}")
                        elif 'generated' in message.lower() and 'report' in message.lower():
                            logger.info(f"   📋 {message}")
                        else:
                            logger.info(f"   📊 {message}")

            logger.info(f"✅ {tenant_id}: Analysis completed successfully")
            logger.info(f"🎉 {tenant_id}: FULLY PROCESSED")
            processed.append(tenant_id)

        except Exception as e:
            logger.error(f"❌ {tenant_id}: FAILED - {e}")
            failed.append(tenant_id)

        logger.info("-" * 40)

    # Final summary
    logger.info("=" * 60)
    logger.info(f"🎯 FINAL SUMMARY:")
    logger.info(f"✅ Successfully processed ({len(processed)}): {processed}")
    if failed:
        logger.error(f"❌ Failed/Skipped ({len(failed)}): {failed}")
    logger.info("=" * 60)

    # Fail the task if no tenants were processed successfully
    if not processed:
        raise Exception(f"❌ No tenants were processed successfully. Failed tenants: {failed}")

    return {"processed": processed, "failed": failed}

# Clean, simple processing task
process_task = PythonOperator(
    task_id='process_selected_tenants',
    python_callable=process_tenants_with_detailed_logs,
    provide_context=True,
    dag=dag,
)

# Completion task
complete_task = PythonOperator(
    task_id='pipeline_complete',
    python_callable=lambda **context: logger.info(
        f"🎉 Punjab Analysis Pipeline completed! "
        f"Execution date: {context['ds']}, Run ID: {context['run_id']}"
    ),
    provide_context=True,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Daily cleanup task - runs after all analysis is complete
cleanup_task = DockerOperator(
    task_id='cleanup_old_data',
    image=DOCKER_IMAGE,
    api_version='auto',
    auto_remove=True,
    entrypoint="python",
    command="cleanup_data.py",
    docker_url='unix://var/run/docker.sock',
    network_mode='host',
    mount_tmp_dir=False,

    # Environment variables for cleanup - no DB credentials
    environment={
        'DATA_DIR': '/data',
        'MAX_AGE_HOURS': '24',  # Delete data older than 24 hours (1 day)
        'DRY_RUN': 'false',  # Set to 'true' for testing
        'CONFIG_PATH': '/run/secrets/db_config.yaml',
        'ENVIRONMENT': 'development',
    },

    # Volume mounts - data directory and secrets
    mounts=[
        {
            'source': '/home/admin1/Desktop/punjab-analysis-fresh/data',
            'target': '/data',
            'type': 'bind'
        },
        {
            'source': '/home/admin1/Desktop/punjab-analysis-fresh/secrets',
            'target': '/run/secrets',
            'type': 'bind',
            'read_only': True
        }
    ],

    dag=dag,
)

# Set up complete dependency chain
start_task >> process_task >> complete_task >> cleanup_task