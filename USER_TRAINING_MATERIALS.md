# Punjab Analysis System - User Training Materials

## 📋 Table of Contents

1. [Getting Started](#getting-started)
2. [Airflow UI Navigation](#airflow-ui-navigation)
3. [DAG Triggering Guide](#dag-triggering-guide)
4. [Monitoring & Status Checking](#monitoring--status-checking)
5. [Output Access & Downloads](#output-access--downloads)
6. [Common User Tasks](#common-user-tasks)
7. [Troubleshooting for Users](#troubleshooting-for-users)
8. [Best Practices](#best-practices)

---

## 🚀 Getting Started

### Prerequisites

- Access to the Punjab Analysis System
- Basic understanding of data processing workflows
- Web browser (Chrome, Firefox, Safari, or Edge)

### System Access

- **Airflow UI URL**: http://192.168.49.2:30082
- **Username**: admin
- **Password**: admin123

### First Login

1. Open your web browser
2. Navigate to http://192.168.49.2:30082
3. Enter credentials:
   - Username: `admin`
   - Password: `admin123`
4. Click "Sign In"

---

## 🌐 Airflow UI Navigation

### Main Dashboard

The Airflow UI provides a comprehensive view of your data processing workflows.

#### Key Sections

- **DAGs**: List of available workflows
- **Grid**: Visual representation of task execution
- **Graph**: Workflow dependency visualization
- **Calendar**: Historical execution timeline
- **Task Instances**: Individual task details
- **Logs**: Execution logs and debugging information

### Navigation Menu

```
┌─────────────────────────────────────────┐
│  🏠 Home    📊 DAGs    📈 Grid    📋 Graph │
│  📅 Calendar  🔍 Task Instances  📝 Logs  │
│  ⚙️  Admin    👤 Profile  🚪 Logout      │
└─────────────────────────────────────────┘
```

### DAG List View

The DAG list shows all available workflows with their current status:

- **Green**: Successfully completed
- **Red**: Failed execution
- **Yellow**: Currently running
- **Gray**: Not started or scheduled

---

## 🎯 DAG Triggering Guide

### Understanding the Punjab Analysis DAG

#### DAG Name

`punjab_analysis_kubernetes_pipeline`

#### DAG Description

Processes property tax data for multiple Punjab tenants (pb.adampur, pb.samana, pb.amloh) through a 5-step pipeline:

1. **start_pipeline**: Initialize and validate parameters
2. **extract_data**: Extract data from PostgreSQL database
3. **analyze_data**: Analyze extracted data and generate insights
4. **pipeline_complete**: Finalize processing and prepare outputs
5. **cleanup_data**: Clean up temporary data

### Manual DAG Triggering

#### Step 1: Navigate to DAG

1. Click on "DAGs" in the navigation menu
2. Find `punjab_analysis_kubernetes_pipeline` in the list
3. Click on the DAG name to open details

#### Step 2: Trigger DAG

1. Click the "Trigger DAG" button (▶️)
2. A configuration dialog will appear

#### Step 3: Configure Parameters

```json
{
  "tenant_ids": ["pb.adampur", "pb.samana", "pb.amloh"],
  "execution_date": "2024-01-01T00:00:00Z",
  "description": "Manual run for data analysis"
}
```

**Parameter Options:**

- **Single Tenant**: `["pb.adampur"]`
- **Multiple Tenants**: `["pb.adampur", "pb.samana"]`
- **All Tenants**: `["pb.adampur", "pb.samana", "pb.amloh"]`

#### Step 4: Execute

1. Click "Trigger" to start the DAG
2. The DAG will appear in the "Running" state
3. Monitor progress in the Grid view

### Programmatic DAG Triggering

#### Using REST API

```bash
# Trigger DAG with specific parameters
curl -X POST \
  -u admin:admin123 \
  -H "Content-Type: application/json" \
  -d '{
    "conf": {
      "tenant_ids": ["pb.adampur", "pb.samana"],
      "execution_date": "2024-01-01T00:00:00Z"
    }
  }' \
  http://192.168.49.2:30082/api/v1/dags/punjab_analysis_kubernetes_pipeline/dagRuns
```

#### Using Python Script

```python
import requests
import json

# Airflow API configuration
AIRFLOW_URL = "http://192.168.49.2:30082/api/v1"
USERNAME = "admin"
PASSWORD = "admin123"

# Trigger DAG
def trigger_dag(tenant_ids, execution_date=None):
    url = f"{AIRFLOW_URL}/dags/punjab_analysis_kubernetes_pipeline/dagRuns"

    payload = {
        "conf": {
            "tenant_ids": tenant_ids,
            "execution_date": execution_date or "2024-01-01T00:00:00Z"
        }
    }

    response = requests.post(
        url,
        auth=(USERNAME, PASSWORD),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )

    if response.status_code == 200:
        print("DAG triggered successfully!")
        return response.json()
    else:
        print(f"Failed to trigger DAG: {response.text}")
        return None

# Example usage
result = trigger_dag(["pb.adampur", "pb.samana"])
```

---

## 📊 Monitoring & Status Checking

### Real-time Monitoring

#### Grid View

1. Click on "Grid" in the navigation menu
2. Select your DAG run from the dropdown
3. View real-time task execution status

**Status Indicators:**

- 🟢 **Success**: Task completed successfully
- 🔴 **Failed**: Task encountered an error
- 🟡 **Running**: Task is currently executing
- ⚪ **Queued**: Task waiting to be executed
- ⚫ **Skipped**: Task was skipped due to conditions

#### Task Details

1. Click on any task in the Grid view
2. View detailed information:
   - Task duration
   - Resource usage
   - Logs and outputs
   - Retry attempts

### Historical Monitoring

#### Calendar View

1. Click on "Calendar" in the navigation menu
2. View DAG execution history
3. Identify patterns and trends

#### Task Instances

1. Click on "Task Instances" in the navigation menu
2. Filter by DAG, date range, or status
3. View detailed execution history

### Status Checking Commands

#### Check DAG Status

```bash
# Get latest DAG run status
curl -u admin:admin123 \
  http://192.168.49.2:30082/api/v1/dags/punjab_analysis_kubernetes_pipeline/dagRuns/latest
```

#### Check Task Status

```bash
# Get task instance status
curl -u admin:admin123 \
  http://192.168.49.2:30082/api/v1/dags/punjab_analysis_kubernetes_pipeline/dagRuns/{dag_run_id}/taskInstances/{task_id}
```

---

## 📁 Output Access & Downloads

### Understanding Outputs

#### Output Types

1. **Analysis Reports**: CSV files with statistical analysis
2. **Data Extracts**: Raw data files from database
3. **Summary Reports**: High-level insights and metrics

#### Output Locations

- **Analysis Reports**: `/output/` directory
- **Data Extracts**: `/data/` directory (temporary)
- **Logs**: Airflow UI and `/opt/airflow/logs/`

### Accessing Outputs

#### Method 1: Airflow UI

1. Navigate to your DAG run
2. Click on the "analyze_data" task
3. View logs for output file locations
4. Download files directly from the UI

#### Method 2: Kubernetes Commands

```bash
# Create a pod to access outputs
kubectl run -it --rm output-viewer \
  --image=busybox \
  --restart=Never \
  -n punjab-analysis \
  -- sh

# Inside the pod, list output files
ls -la /output/
ls -la /data/

# Copy files to local machine
kubectl cp punjab-analysis/output-viewer:/output/analysis_summary.csv ./analysis_summary.csv
```

#### Method 3: REST API

```bash
# Get task logs (contains output file paths)
curl -u admin:admin123 \
  http://192.168.49.2:30082/api/v1/dags/punjab_analysis_kubernetes_pipeline/dagRuns/{dag_run_id}/taskInstances/analyze_data/logs/1
```

### Output File Formats

#### Analysis Reports

```csv
tenant_id,metric_name,metric_value,calculation_date
pb.adampur,total_properties,1250,2024-01-01
pb.adampur,average_tax,15000.50,2024-01-01
pb.samana,total_properties,980,2024-01-01
pb.samana,average_tax,12000.75,2024-01-01
```

#### Data Extracts

```csv
property_id,tenant_id,property_type,tax_amount,created_date
12345,pb.adampur,residential,15000,2023-12-01
12346,pb.adampur,commercial,25000,2023-12-02
12347,pb.samana,residential,12000,2023-12-03
```

---

## 🔧 Common User Tasks

### 1. Running Analysis for Specific Tenant

#### Single Tenant Analysis

```json
{
  "tenant_ids": ["pb.adampur"],
  "execution_date": "2024-01-01T00:00:00Z"
}
```

#### Steps:

1. Trigger DAG with single tenant parameter
2. Monitor execution in Grid view
3. Download results from output directory

### 2. Running Analysis for Multiple Tenants

#### Multiple Tenant Analysis

```json
{
  "tenant_ids": ["pb.adampur", "pb.samana"],
  "execution_date": "2024-01-01T00:00:00Z"
}
```

#### Steps:

1. Trigger DAG with multiple tenant parameters
2. Monitor parallel execution
3. Collect results for each tenant

### 3. Scheduling Regular Analysis

#### Setting Up Schedules

1. Navigate to DAG details
2. Click on "Schedule" tab
3. Set cron expression for regular execution
4. Enable the DAG

#### Common Schedules:

- **Daily**: `0 2 * * *` (2 AM daily)
- **Weekly**: `0 2 * * 1` (2 AM every Monday)
- **Monthly**: `0 2 1 * *` (2 AM on 1st of every month)

### 4. Monitoring Long-Running Jobs

#### Best Practices:

1. Check Grid view every 30 minutes
2. Monitor resource usage
3. Set up alerts for failures
4. Keep logs for troubleshooting

### 5. Handling Failed Jobs

#### When a Job Fails:

1. Check task logs for error details
2. Identify the root cause
3. Fix configuration if needed
4. Retry the failed task or entire DAG

#### Retry Options:

- **Retry Single Task**: Click on failed task → "Retry"
- **Retry Entire DAG**: Trigger new DAG run
- **Clear and Retry**: Clear task state and retry

---

## 🚨 Troubleshooting for Users

### Common Issues

#### 1. DAG Not Appearing

**Symptoms**: DAG not visible in the DAG list
**Solutions**:

- Refresh the browser page
- Check if DAG is enabled (toggle switch)
- Contact system administrator

#### 2. DAG Trigger Fails

**Symptoms**: Cannot trigger DAG manually
**Solutions**:

- Check DAG configuration
- Verify parameters are valid JSON
- Ensure DAG is not already running

#### 3. Tasks Stuck in Running State

**Symptoms**: Tasks show as running for extended periods
**Solutions**:

- Check task logs for errors
- Monitor resource usage
- Contact system administrator if stuck > 2 hours

#### 4. Cannot Access Outputs

**Symptoms**: Output files not found or inaccessible
**Solutions**:

- Check task completion status
- Verify output directory permissions
- Use alternative access methods

### Error Messages

#### "DAG not found"

- Verify DAG name is correct
- Check if DAG is enabled
- Refresh the page

#### "Invalid configuration"

- Check JSON syntax in parameters
- Verify tenant IDs are valid
- Use provided parameter examples

#### "Task failed"

- Check task logs for detailed error
- Verify input data availability
- Contact system administrator

### Getting Help

#### Self-Service Options:

1. Check this training guide
2. Review task logs in Airflow UI
3. Use troubleshooting commands

#### Escalation:

1. Contact system administrator
2. Provide DAG run ID and error details
3. Include relevant logs and screenshots

---

## 📚 Best Practices

### 1. DAG Execution

#### Before Triggering:

- Verify tenant IDs are correct
- Check system resources
- Ensure no conflicting runs

#### During Execution:

- Monitor progress regularly
- Don't trigger multiple runs simultaneously
- Keep logs for reference

#### After Execution:

- Verify outputs are generated
- Download results promptly
- Clean up if needed

### 2. Parameter Configuration

#### Valid Tenant IDs:

- `pb.adampur`
- `pb.samana`
- `pb.amloh`

#### Parameter Format:

```json
{
  "tenant_ids": ["pb.adampur"],
  "execution_date": "2024-01-01T00:00:00Z",
  "description": "User description"
}
```

### 3. Monitoring

#### Regular Checks:

- Monitor DAG execution every 30 minutes
- Check for error notifications
- Verify output generation

#### Performance Optimization:

- Run during off-peak hours
- Use appropriate tenant combinations
- Monitor resource usage

### 4. Data Management

#### Output Handling:

- Download results promptly
- Store in appropriate locations
- Maintain backup copies

#### Cleanup:

- Remove old output files
- Archive important results
- Follow data retention policies

---

## 🎓 Training Exercises

### Exercise 1: Basic DAG Triggering

**Objective**: Trigger a DAG for a single tenant
**Steps**:

1. Log into Airflow UI
2. Navigate to the Punjab Analysis DAG
3. Trigger with `["pb.adampur"]` parameter
4. Monitor execution in Grid view
5. Download results

### Exercise 2: Multi-Tenant Analysis

**Objective**: Run analysis for multiple tenants
**Steps**:

1. Trigger DAG with `["pb.adampur", "pb.samana"]`
2. Monitor parallel execution
3. Compare results between tenants
4. Generate summary report

### Exercise 3: Error Handling

**Objective**: Handle a failed task
**Steps**:

1. Trigger DAG with invalid parameters
2. Observe failure in Grid view
3. Check task logs for error details
4. Retry with correct parameters

### Exercise 4: Output Analysis

**Objective**: Analyze and interpret results
**Steps**:

1. Download analysis reports
2. Review statistical metrics
3. Identify trends and patterns
4. Generate insights

---

_This training material provides comprehensive guidance for users of the Punjab Analysis System. For additional support, contact the system administrator or refer to the technical documentation._
