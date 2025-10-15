# Punjab Property Tax Analysis System - Technical Documentation

## 📋 Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Component Details](#component-details)
4. [Data Flow](#data-flow)
5. [Configuration Management](#configuration-management)
6. [Storage Architecture](#storage-architecture)
7. [Security Model](#security-model)
8. [API Reference](#api-reference)
9. [Deployment Architecture](#deployment-architecture)
10. [Monitoring & Observability](#monitoring--observability)

---

## 🎯 System Overview

### Purpose

The Punjab Property Tax Analysis System is a production-ready, scalable data pipeline that processes property tax data from multiple Punjab tenants using Apache Airflow orchestrated on Kubernetes.

### Key Capabilities

- **Multi-tenant Data Processing**: Handles pb.adampur, pb.samana, pb.amloh tenants
- **Scalable Architecture**: Kubernetes-native with dynamic pod allocation
- **Data Pipeline**: Extract → Analyze → Report → Cleanup workflow
- **Persistent Storage**: Data preservation across runs
- **Production Ready**: Monitoring, logging, error handling, and security

### Technology Stack

- **Orchestration**: Apache Airflow 2.5.3
- **Container Platform**: Kubernetes (Minikube for development)
- **Database**: PostgreSQL 13
- **Language**: Python 3.11
- **Storage**: Persistent Volume Claims (PVCs)
- **Configuration**: YAML-based with environment overrides

---

## 🏗️ Architecture

### High-Level Architecture

```
┌───────────────────────────────────────────────────────────────┐
│  Minikube Kubernetes Cluster                                  │
│  Namespace: punjab-analysis                                   │
├───────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Airflow       │  │   PostgreSQL    │  │   Punjab Data   │ │
│  │   Webserver     │  │   Database      │  │   Processing    │ │
│  │   (Port 30082)  │  │   (Port 5436)   │  │   Pods          │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│           │                     │                     │        │
│           └─────────────────────┼─────────────────────┘        │
│                                 │                              │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Persistent Storage Layer                       │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │ │
│  │  │   Data PVC  │ │  Output PVC │ │  Logs PVC   │           │ │
│  │  │   (10GB)    │ │   (5GB)     │ │   (5GB)     │           │ │
│  │  └─────────────┘ └─────────────┘ └─────────────┘           │ │
│  │  ┌─────────────┐ ┌─────────────┐                           │ │
│  │  │ Secrets PVC │ │ Postgres PVC│                           │ │
│  │  │   (1GB)     │ │   (5GB)     │                           │ │
│  │  └─────────────┘ └─────────────┘                           │ │
│  └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
```

### Component Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                    Airflow DAG Layer                          │
├───────────────────────────────────────────────────────────────┤
│  start_pipeline → extract_data → analyze_data →              │
│  pipeline_complete → cleanup_data                            │
└───────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────┐
│                 Kubernetes Pod Layer                          │
├───────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐           │
│  │   Python    │  │ Kubernetes  │  │ Kubernetes  │           │
│  │  Operator   │  │    Pod      │  │    Pod      │           │
│  │ (Lightweight)│  │ (Heavy CPU) │  │ (Heavy CPU) │           │
│  └─────────────┘  └─────────────┘  └─────────────┘           │
└───────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────┐
│                  Application Layer                            │
├───────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐           │
│  │   Config    │  │   Extract   │  │   Analyze   │           │
│  │   Loader    │  │   Module    │  │   Module    │           │
│  └─────────────┘  └─────────────┘  └─────────────┘           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐           │
│  │   Cleanup   │  │  Filestore  │  │   Process   │           │
│  │   Module    │  │  Uploader   │  │   All       │           │
│  └─────────────┘  └─────────────┘  └─────────────┘           │
└───────────────────────────────────────────────────────────────┘
```

---

## 🔧 Component Details

### 1. Airflow Components

#### Webserver

- **Purpose**: Web UI and REST API for DAG management
- **Image**: apache/airflow:2.5.3
- **Port**: 30082 (NodePort)
- **Resources**: 1 CPU, 2GB RAM
- **Access**: http://192.168.49.2:30082

#### Scheduler

- **Purpose**: DAG parsing and task orchestration
- **Image**: apache/airflow:2.5.3
- **Resources**: 1 CPU, 2GB RAM
- **Dependencies**: PostgreSQL for metadata storage

### 2. Application Components

#### PunjabDataExtractor

```python
# File: src/extract_data.py
# Purpose: Extract property tax data from PostgreSQL
# Key Features:
- Multi-tenant support (pb.adampur, pb.samana, pb.amloh)
- Chunked processing (50,000 records per chunk)
- CSV output generation
- Error handling and retry logic
```

#### PunjabDataAnalyzer

```python
# File: src/analyze_data.py
# Purpose: Analyze extracted data and generate insights
# Key Features:
- Statistical analysis (counts, averages, distributions)
- Data quality checks
- Report generation (CSV format)
- Tenant-specific analysis
```

#### PunjabDataCleanup

```python
# File: src/cleanup_data.py
# Purpose: Clean up old data based on retention policies
# Key Features:
- Age-based cleanup (24 hours default)
- Dry-run mode for testing
- Selective cleanup by tenant
- Logging of cleanup operations
```

#### ConfigLoader

```python
# File: src/config_loader.py
# Purpose: Centralized configuration management
# Key Features:
- 3-layer configuration system
- Environment-specific overrides
- Database connection management
- Tenant-specific settings
```

### 3. Storage Components

#### Persistent Volume Claims (PVCs)

| PVC Name             | Size | Access Mode | Purpose                  | Mount Path                 |
| -------------------- | ---- | ----------- | ------------------------ | -------------------------- |
| `punjab-data-pvc`    | 10GB | RWX         | Temporary extracted data | `/data`                    |
| `punjab-output-pvc`  | 5GB  | RWX         | Analysis reports         | `/output`                  |
| `punjab-secrets-pvc` | 1GB  | ROX         | Configuration files      | `/app/secrets`             |
| `airflow-logs-pvc`   | 5GB  | RWX         | Airflow task logs        | `/opt/airflow/logs`        |
| `postgres-pvc`       | 5GB  | RWO         | Database data            | `/var/lib/postgresql/data` |

---

## 📊 Data Flow

### 1. Pipeline Execution Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   start_pipeline│───▶│  extract_data   │───▶│  analyze_data   │
│   (Python)      │    │  (K8s Pod)      │    │  (K8s Pod)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ pipeline_complete│◀───│  cleanup_data   │◀───│   Filestore    │
│   (Python)      │    │  (K8s Pod)      │          upload      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 2. Data Processing Flow

```
PostgreSQL Database
        │
        ▼
┌─────────────────┐
│   Extract Data  │
│   (CSV Files)   │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│   Analyze Data  │
│   (Statistics)  │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  Generate CSV   │
│   Reports       │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│   Cleanup Old   │
│     Data        │
└─────────────────┘
```

### 3. Multi-Tenant Processing

```
Tenant List: [pb.adampur, pb.samana, pb.amloh]
        │
        ▼
┌─────────────────┐
│  For Each Tenant│
│                 │
│  ┌─────────────┐│
│  │ Extract     ││
│  │ Data        ││
│  └─────────────┘│
│  ┌─────────────┐│
│  │ Analyze     ││
│  │ Data        ││
│  └─────────────┘│
│  ┌─────────────┐│
│  │ Generate    ││
│  │ Reports     ││
│  └─────────────┘│
└─────────────────┘
```

---

## ⚙️ Configuration Management

### 1. Configuration Hierarchy

```
1. Airflow Variables (Highest Priority)
   ↓
2. Environment Variables
   ↓
3. ConfigMap Values
   ↓
4. Default Values (Lowest Priority)
```

### 2. Configuration Sources

#### Environment Variables

```bash
CONFIG_PATH=/app/secrets/db_config.yaml
ENVIRONMENT=development
TENANT_IDS=["pb.adampur","pb.samana","pb.amloh"]
OUTPUT_DIR=/output
DATA_DIR=/data
```

#### ConfigMap (k8s/10-dag-configmap.yaml)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: punjab-kubernetes-dag
data:
  punjab_analysis_kubernetes_dag.py: |
    # DAG definition
```

#### Secrets (k8s/secrets/db_config.yaml)

```yaml
database:
  host: postgres-external.punjab-analysis.svc.cluster.local
  port: 5436
  name: airflowdb
  user: postgres
  password: postgres
```

### 3. Tenant-Specific Configuration

```yaml
tenants:
  pb.adampur:
    priority: high
    chunk_size: 25000
  pb.samana:
    priority: medium
    chunk_size: 50000
  pb.amloh:
    priority: medium
    chunk_size: 50000
```

---

## 💾 Storage Architecture

### 1. Volume Mounts

#### Application Pods

```yaml
volumeMounts:
  - name: data-volume
    mountPath: /data
  - name: output-volume
    mountPath: /output
  - name: secrets-volume
    mountPath: /app/secrets
    readOnly: true
```

#### Airflow Pods

```yaml
volumeMounts:
  - name: logs-volume
    mountPath: /opt/airflow/logs
  - name: dags-volume
    mountPath: /opt/airflow/dags
```

### 2. Data Lifecycle

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Extract       │───▶│   Analyze       │───▶│   Output        │
│   (Temporary)   │    │   (Temporary)   │    │   (Persistent)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Cleanup       │    │   Cleanup       │    │   Retention     │
│   (24h)         │    │   (24h)         │    │   (Permanent)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## 🔒 Security Model

### 1. RBAC (Role-Based Access Control)

#### Service Accounts

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow-worker
  namespace: punjab-analysis
```

#### Roles

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: airflow-worker-role
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch", "create", "delete"]
```

### 2. Pod Security Contexts

```yaml
securityContext:
  runAsUser: 1000
  runAsGroup: 1000
  runAsNonRoot: true
  fsGroup: 1000
```

### 3. Network Security

#### Service Isolation

- All services run in `punjab-analysis` namespace
- Internal communication via Kubernetes services
- External access only through NodePort (30082)

---

## 🔌 API Reference

### 1. Airflow REST API

#### Base URL

```
http://192.168.49.2:30082/api/v1/
```

#### Authentication

```bash
# Basic Auth
curl -u admin:admin123 http://192.168.49.2:30082/api/v1/dags
```

#### Key Endpoints

##### List DAGs

```bash
GET /dags
```

##### Trigger DAG

```bash
POST /dags/{dag_id}/dagRuns
Content-Type: application/json

{
  "conf": {
    "tenant_ids": ["pb.adampur", "pb.samana"],
    "execution_date": "2024-01-01T00:00:00Z"
  }
}
```

##### Get DAG Run Status

```bash
GET /dags/{dag_id}/dagRuns/{dag_run_id}
```

##### Get Task Logs

```bash
GET /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{log_id}
```

### 2. Application APIs

#### Process All Tenants

```python
# Entry point for multi-tenant processing
python process_all_tenants.py
```

#### Single Mode Processing

```python
# Extract mode
MODE=extract TENANT_ID=pb.adampur python main.py

# Analyze mode
MODE=analyze TENANT_ID=pb.adampur python main.py
```

---

## 🚀 Deployment Architecture

### 1. Kubernetes Manifests

#### Core Components

```
k8s/
├── 01-namespace.yaml              # Namespace definition
├── 05-persistent-volumes.yaml     # Storage volumes
├── 06-persistent-volume-claims.yaml # Storage claims
├── 07-airflow-deployment.yaml     # Airflow components
├── 08-airflow-service.yaml        # Web service
├── 09-rbac.yaml                   # Security
├── 10-dag-configmap.yaml          # DAG definition
├── 11-pod-template.yaml           # Pod templates
├── 12-pod-template-configmap.yaml # Pod config
└── postgres-deployment.yaml       # Database
```

### 2. Deployment Process

```bash
# 1. Build and load image
cd src && ./build.sh
minikube image load punjab-analysis:v3.0.0

# 2. Deploy to Kubernetes
cd k8s && ./deploy.sh

# 3. Verify deployment
kubectl get pods -n punjab-analysis
```

### 3. Scaling Considerations

#### Horizontal Scaling

- Multiple Airflow scheduler replicas
- Dynamic pod creation for tasks
- Load balancing across nodes

#### Vertical Scaling

- Resource limits per pod
- Memory and CPU allocation
- Storage capacity planning

---

## 📈 Monitoring & Observability

### 1. Logging

#### Log Sources

- **Airflow Logs**: Task execution logs
- **Application Logs**: Python application output
- **System Logs**: Kubernetes pod logs

#### Log Locations

```
/opt/airflow/logs/          # Airflow task logs
/data/                      # Application data logs
/output/                    # Analysis output logs
```

### 2. Monitoring

#### Health Checks

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
```

#### Resource Monitoring

```bash
# Pod resource usage
kubectl top pods -n punjab-analysis

# Node resource usage
kubectl top nodes
```

### 3. Alerting

#### Key Metrics

- DAG run success/failure rates
- Task execution times
- Resource utilization
- Storage capacity

#### Alert Conditions

- DAG run failures > 5%
- Task execution time > 2 hours
- Memory usage > 80%
- Storage usage > 90%

---

## 🔧 Development & Maintenance

### 1. Code Structure

```
src/
├── main.py                   # Entry point
├── extract_data.py          # Data extraction
├── analyze_data.py          # Data analysis
├── cleanup_data.py          # Data cleanup
├── config_loader.py         # Configuration
├── process_all_tenants.py   # Multi-tenant processing
├── filestore_uploader.py    # File upload
├── requirements.txt         # Dependencies
└── Dockerfile              # Container image
```

### 2. Testing

#### Unit Tests

```bash
# Run tests
python -m pytest tests/

# Coverage report
python -m pytest --cov=src tests/
```

#### Integration Tests

```bash
# Test database connection
python -c "from config_loader import ConfigLoader; ConfigLoader().validate_config()"

# Test data extraction
MODE=extract TENANT_ID=pb.adampur python main.py
```

### 3. Maintenance Tasks

#### Regular Maintenance

- Log rotation and cleanup
- Database maintenance
- Image updates
- Security patches

#### Backup Procedures

- Database backups
- Configuration backups
- PVC data backups

---

_This technical documentation provides a comprehensive overview of the Punjab Property Tax Analysis System. For specific implementation details, refer to the individual component documentation and source code._
