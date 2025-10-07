# Punjab Property Tax Analysis System - Kubernetes Edition

A production-ready, scalable data pipeline for extracting and analyzing property tax data from multiple Punjab tenants using Apache Airflow and Kubernetes.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup Guide](#detailed-setup-guide)
- [Usage](#usage)
- [Kubernetes Resources](#kubernetes-resources)
- [Database](#database)
- [Accessing Outputs](#accessing-outputs)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [API Reference](#api-reference)
- [Commands Cheat Sheet](#commands-cheat-sheet)
- [Migration from Docker](#migration-from-docker)

---

## 🎯 Overview

This system processes property tax data for multiple Punjab tenants (pb.adampur, pb.samana, pb.amloh) by:

1. **Extracting** raw data from PostgreSQL database
2. **Analyzing** the data to generate insights
3. **Generating** CSV reports with analysis results
4. **Cleaning up** old data automatically

### Key Features

- ✅ **Kubernetes-Native**: Runs entirely on Kubernetes for scalability and fault tolerance
- ✅ **Multi-Tenant Support**: Process single or multiple tenants in one execution
- ✅ **Dynamic Resource Allocation**: Pods are created on-demand and cleaned up automatically
- ✅ **Persistent Storage**: Data, outputs, and logs are preserved across runs
- ✅ **Production-Ready**: Includes monitoring, logging, and error handling
- ✅ **Simplified DAG**: Clean 5-task workflow instead of complex per-tenant task groups

---

## 🏗️ Architecture

```
┌───────────────────────────────────────────────────────────────┐
│  Minikube Kubernetes Cluster                                  │
│  Namespace: punjab-analysis                                   │
├───────────────────────────────────────────────────────────────┤
│                                                                │
│  ┌─────────────────┐  ┌─────────────────┐                    │
│  │ Airflow         │  │ Airflow         │                    │
│  │ Webserver       │  │ Scheduler       │                    │
│  │ (UI + API)      │  │ (Orchestrator)  │                    │
│  │ Port: 30082     │  │                 │                    │
│  └─────────────────┘  └─────────────────┘                    │
│                                                                │
│  ┌─────────────────┐  ┌─────────────────┐                    │
│  │  PostgreSQL     │  │  Analysis Pods  │                    │
│  │  (Airflow DB +  │  │  (Created on    │                    │
│  │   Punjab Data)  │  │   Demand)       │                    │
│  │  Port: 5436     │  │                 │                    │
│  └─────────────────┘  └─────────────────┘                    │
│                                                                │
│  Persistent Volumes:                                          │
│  • Data PVC (10Gi) - Extracted CSV files                      │
│  • Output PVC (5Gi) - Analysis reports                        │
│  • Secrets PVC (1Gi) - Database credentials                   │
│  • Logs PVC (5Gi) - Airflow task logs                         │
│  • Postgres PVC (5Gi) - Database storage                      │
└───────────────────────────────────────────────────────────────┘
```

### DAG Workflow

```
start_pipeline
    ↓
extract_data (processes all selected tenants)
    ↓
analyze_data (processes all selected tenants)
    ↓
pipeline_complete
    ↓
cleanup_old_data
```

**Execution Flow for Multiple Tenants:**
```
User triggers: {"tenant_ids": ["pb.adampur", "pb.amloh"]}
   ↓
start_pipeline: Validates and stores tenant list
   ↓
extract_data pod: Processes BOTH tenants sequentially in ONE pod
   ├─ pb.adampur: Extract → Save to /data/adampur/
   └─ pb.amloh: Extract → Save to /data/amloh/
   ↓
analyze_data pod: Processes BOTH tenants sequentially in ONE pod
   ├─ pb.adampur: Analyze → Generate report
   └─ pb.amloh: Analyze → Generate report
   ↓
pipeline_complete: Success notification
   ↓
cleanup_old_data: Remove files older than 24 hours
```

---

## 📦 Prerequisites

### System Requirements

- **OS**: Linux (tested on Ubuntu)
- **RAM**: Minimum 8GB (16GB recommended)
- **CPU**: Minimum 4 cores
- **Disk**: Minimum 30GB free space

### Required Software

```bash
# Docker
docker --version  # >= 20.10

# Minikube
minikube version  # >= 1.30

# kubectl
kubectl version --client  # >= 1.25

# PostgreSQL Client (for data import)
psql --version  # >= 13
```

### Installation

```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
newgrp docker

# Install Minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
```

---

## 🚀 Quick Start

### 1. Start Minikube

```bash
minikube start --memory=8192 --cpus=4
minikube status
```

### 2. Clone and Navigate to Project

```bash
cd /home/admin1/Desktop/airflow-punjab-analysis
```

### 3. Build Docker Image

```bash
cd src
docker build -t punjab-analysis:v3.0.0 .
minikube image load punjab-analysis:v3.0.0
cd ..
```

### 4. Deploy to Kubernetes

```bash
# Create namespace
kubectl create namespace punjab-analysis

# Deploy all components
kubectl apply -f k8s/

# Wait for pods to be ready
kubectl wait --for=condition=ready pod -l app=airflow -n punjab-analysis --timeout=300s
```

### 5. Access Airflow UI

```bash
# Get Airflow URL
minikube service airflow-webserver -n punjab-analysis --url

# Open in browser
# Default credentials: admin/admin
```

### 6. Trigger Your First Pipeline

```bash
# Via UI: Click on "punjab_analysis_kubernetes_pipeline" → Play button

# Via CLI:
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow dags trigger punjab_analysis_kubernetes_pipeline \
  -c '{"tenant_id": "pb.amloh"}'
```

---

## 📚 Detailed Setup Guide

### Step 1: Initialize Kubernetes Cluster

```bash
# Start Minikube with sufficient resources
minikube start --memory=8192 --cpus=4 --disk-size=30g

# Verify cluster
minikube status
kubectl cluster-info
```

### Step 2: Create Namespace

```bash
kubectl create namespace punjab-analysis
kubectl config set-context --current --namespace=punjab-analysis
```

### Step 3: Build Application Image

```bash
cd /home/admin1/Desktop/airflow-punjab-analysis/src

# Build Docker image
docker build -t punjab-analysis:v3.0.0 .

# Load into Minikube
minikube image load punjab-analysis:v3.0.0

# Verify image is loaded
minikube image ls | grep punjab
```

### Step 4: Deploy PostgreSQL

```bash
kubectl apply -f k8s/postgres-deployment.yaml

# Wait for PostgreSQL to be ready
kubectl wait --for=condition=ready pod -l app=postgres -n punjab-analysis --timeout=120s

# Verify deployment
kubectl get pods -n punjab-analysis | grep postgres
```

### Step 5: Import Punjab Data

```bash
# Export data from local PostgreSQL (if you have existing data)
PGPASSWORD=postgres pg_dump -h localhost -p 5432 -U postgres -d airflowdb \
  -t 'eg_pt_*' -t 'egbs_*' --no-owner --no-acl -f /tmp/punjab_data.sql

# Get postgres pod name
POD=$(kubectl get pod -n punjab-analysis -l app=postgres -o jsonpath='{.items[0].metadata.name}')

# Copy SQL file to pod
kubectl cp /tmp/punjab_data.sql punjab-analysis/$POD:/tmp/punjab_data.sql

# Import into Kubernetes PostgreSQL
kubectl exec -n punjab-analysis $POD -- \
  psql -U airflow -d airflow -f /tmp/punjab_data.sql

# Verify data import
kubectl exec -n punjab-analysis $POD -- \
  psql -U airflow -d airflow -c "SELECT COUNT(*) FROM eg_pt_property;"
```

### Step 6: Configure Database Secrets

```bash
# Update secrets PVC with database configuration
kubectl apply -f k8s/update-secrets.yaml

# Or copy config file to secrets PVC
POD=$(kubectl get pod -n punjab-analysis -l app=postgres -o jsonpath='{.items[0].metadata.name}')
kubectl cp secrets/db_config.yaml punjab-analysis/$POD:/tmp/db_config.yaml
```

### Step 7: Create DAG ConfigMap

```bash
kubectl create configmap punjab-kubernetes-dag \
  --from-file=airflow/dags/punjab_analysis_kubernetes_dag.py \
  -n punjab-analysis
```

### Step 8: Deploy Airflow Components

```bash
# Deploy scheduler
kubectl apply -f k8s/airflow-scheduler-deployment.yaml

# Deploy webserver
kubectl apply -f k8s/airflow-webserver-deployment.yaml

# Wait for all Airflow components
kubectl wait --for=condition=ready pod -l app=airflow -n punjab-analysis --timeout=300s
```

### Step 9: Fix PVC Permissions

```bash
# Run permission fix job (pods run as UID 1000)
kubectl apply -f k8s/fix-pvc-permissions.yaml

# Verify job completed
kubectl get jobs -n punjab-analysis
```

### Step 10: Access Airflow UI

```bash
# Get service URL
minikube service airflow-webserver -n punjab-analysis --url

# Output: http://192.168.49.2:30082

# Open in browser and login with:
# Username: admin
# Password: admin
```

---

## 💻 Usage

### Triggering the Pipeline

#### Method 1: Via Airflow UI

1. Open Airflow UI: `http://192.168.49.2:30082`
2. Navigate to DAGs page
3. Find `punjab_analysis_kubernetes_pipeline`
4. Click the **Play** button
5. Configure trigger with JSON:
   ```json
   {
     "tenant_id": "pb.amloh"
   }
   ```
   Or for multiple tenants:
   ```json
   {
     "tenant_ids": ["pb.adampur", "pb.amloh"]
   }
   ```

#### Method 2: Via kubectl Command

```bash
# Single tenant
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow dags trigger punjab_analysis_kubernetes_pipeline \
  -c '{"tenant_id": "pb.amloh"}'

# Multiple tenants
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow dags trigger punjab_analysis_kubernetes_pipeline \
  -c '{"tenant_ids": ["pb.adampur", "pb.amloh", "pb.samana"]}'

# All tenants (default)
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow dags trigger punjab_analysis_kubernetes_pipeline
```

#### Method 3: Via REST API

```bash
# Get base URL
BASE_URL="http://192.168.49.2:30082/api/v1"

# Trigger DAG
curl -X POST "$BASE_URL/dags/punjab_analysis_kubernetes_pipeline/dagRuns" \
  --user admin:admin \
  -H "Content-Type: application/json" \
  -d '{
    "conf": {
      "tenant_id": "pb.amloh"
    }
  }'
```

### Monitoring Pipeline Execution

```bash
# Watch pod creation/deletion
kubectl get pods -n punjab-analysis -w

# View DAG runs
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow dags list-runs -d punjab_analysis_kubernetes_pipeline

# Check task states for specific run
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow tasks states-for-dag-run punjab_analysis_kubernetes_pipeline <run_id>
```

---

## 🧩 Kubernetes Resources

### Pods

| Pod | Purpose | Replicas | Resources |
|-----|---------|----------|-----------|
| airflow-webserver | Airflow UI & REST API | 1 | 1Gi RAM, 1 CPU |
| airflow-scheduler | DAG orchestration | 1 | 2Gi RAM, 2 CPUs |
| postgres | Database | 1 | 2Gi RAM, 1 CPU |
| extract-all-tenants | Data extraction (dynamic) | 0-1 | 1-4Gi RAM, 1-4 CPUs |
| analyze-all-tenants | Data analysis (dynamic) | 0-1 | 2-8Gi RAM, 2-8 CPUs |
| cleanup-data | Old data cleanup (dynamic) | 0-1 | 256Mi-512Mi RAM |

### Services

| Service | Type | Port | Purpose |
|---------|------|------|---------|
| airflow-webserver | NodePort | 8080:30082 | External access to Airflow UI |
| airflow-postgres | ClusterIP | 5436 | Internal database access |

### Persistent Volume Claims

| PVC | Size | Access Mode | Mount Path | Purpose |
|-----|------|-------------|------------|---------|
| punjab-data-pvc | 10Gi | ReadWriteMany | /data | Extracted CSV data |
| punjab-output-pvc | 5Gi | ReadWriteMany | /output | Analysis reports |
| punjab-secrets-pvc | 1Gi | ReadOnlyMany | /app/secrets | DB credentials |
| airflow-logs-pvc | 5Gi | ReadWriteMany | /opt/airflow/logs | Airflow logs |
| postgres-pvc | 5Gi | ReadWriteOnce | /var/lib/postgresql/data | PostgreSQL data |

**Total Storage**: 26Gi

---

## 🗄️ Database

### PostgreSQL Configuration

```yaml
Host: airflow-postgres.punjab-analysis.svc.cluster.local
Port: 5436
Database: airflow
Username: airflow
Password: airflow
```

### Database Schema

```sql
-- Property Tax Tables (Punjab Data)
eg_pt_property          -- 33,502 records (16 MB)
eg_pt_owner             -- 66,100 records (23 MB)
eg_pt_unit              -- 61,368 records (10 MB)
eg_pt_address           -- Address data (16 KB)
egbs_demand_v1          -- 203,257 records (77 MB)
egbs_demanddetail_v1    -- 2,565,788 records (755 MB)

Total Punjab Data: ~881 MB

-- Airflow Metadata Tables
dag, dag_run, task_instance, job, log, xcom, etc.
```

### Accessing Database

```bash
# Method 1: kubectl exec
kubectl exec -it -n punjab-analysis deployment/postgres -- \
  psql -U airflow -d airflow

# Method 2: Port forwarding
kubectl port-forward -n punjab-analysis service/airflow-postgres 5436:5436 &
psql -h localhost -p 5436 -U airflow -d airflow

# Method 3: From Python
python3 << 'EOF'
import psycopg2
conn = psycopg2.connect(
    host='localhost',
    port=5436,
    user='airflow',
    password='airflow',
    database='airflow'
)
cursor = conn.cursor()
cursor.execute("SELECT tenantid, COUNT(*) FROM eg_pt_property GROUP BY tenantid;")
print(cursor.fetchall())
conn.close()
EOF
```

### Useful SQL Queries

```sql
-- Count properties by tenant
SELECT tenantid, COUNT(*)
FROM eg_pt_property
GROUP BY tenantid;

-- Check data freshness
SELECT tenantid,
       COUNT(*) as total,
       MAX(createdtime) as last_updated
FROM eg_pt_property
GROUP BY tenantid;

-- Database size
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;
```

---

## 📂 Accessing Outputs

### Method 1: Create Debug Pod

```bash
# Create pod to access output PVC
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: output-viewer
  namespace: punjab-analysis
spec:
  containers:
  - name: viewer
    image: busybox
    command: ["sh", "-c", "sleep 3600"]
    volumeMounts:
    - name: output
      mountPath: /output
    - name: data
      mountPath: /data
  volumes:
  - name: output
    persistentVolumeClaim:
      claimName: punjab-output-pvc
  - name: data
    persistentVolumeClaim:
      claimName: punjab-data-pvc
  restartPolicy: Never
EOF

# List output files
kubectl exec -n punjab-analysis output-viewer -- ls -lh /output/

# List extracted data
kubectl exec -n punjab-analysis output-viewer -- ls -lh /data/amloh/
```

### Method 2: Copy Files to Local

```bash
# Copy specific file
kubectl cp punjab-analysis/output-viewer:/output/Punjab_Data_Analysis_amloh_20251006.csv \
  ./local-reports/amloh_latest.csv

# Copy entire directory
kubectl exec -n punjab-analysis output-viewer -- tar czf /tmp/all-outputs.tar.gz /output/
kubectl cp punjab-analysis/output-viewer:/tmp/all-outputs.tar.gz ./all-outputs.tar.gz
tar xzf all-outputs.tar.gz
```

### Output File Structure

```
/output/
├── Punjab_Data_Analysis_adampur_YYYYMMDD_HHMMSS.csv    (~671 KB)
├── Punjab_Data_Analysis_amloh_YYYYMMDD_HHMMSS.csv      (~683 KB)
├── Punjab_Data_Analysis_samana_YYYYMMDD_HHMMSS.csv     (~3.0 MB)
└── SUMMARY_REPORT_YYYYMMDD_HHMMSS.csv                  (small)

/data/
├── adampur/
│   ├── eg_pt_property.csv
│   ├── eg_pt_owner.csv
│   ├── eg_pt_unit.csv
│   ├── egbs_demand_v1/
│   │   └── output_0.csv
│   └── egbs_demanddetail_v1/
│       ├── output_0.csv
│       ├── output_1.csv
│       └── ...
├── amloh/
│   └── (same structure)
└── samana/
    └── (same structure)
```

---

## 🔍 Monitoring & Troubleshooting

### Check System Health

```bash
# Overall cluster status
minikube status
kubectl cluster-info

# Check all pods
kubectl get pods -n punjab-analysis

# Check resource usage
kubectl top nodes  # Requires metrics-server
kubectl top pods -n punjab-analysis

# Check events
kubectl get events -n punjab-analysis --sort-by='.lastTimestamp'
```

### View Logs

```bash
# Airflow webserver logs
kubectl logs -n punjab-analysis deployment/airflow-webserver --tail=100

# Airflow scheduler logs
kubectl logs -n punjab-analysis deployment/airflow-scheduler --tail=100

# PostgreSQL logs
kubectl logs -n punjab-analysis deployment/postgres --tail=100

# Dynamic pod logs (while running)
kubectl logs -n punjab-analysis extract-all-tenants-xxxxx --tail=100 -f

# Task logs via Airflow
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow tasks logs punjab_analysis_kubernetes_pipeline extract_data <run_id>
```

### Common Issues & Solutions

#### Issue 1: Pod Stuck in Pending

```bash
# Check why pod is pending
kubectl describe pod <pod-name> -n punjab-analysis

# Common causes:
# - Insufficient resources → Increase Minikube memory
# - PVC not bound → Check PVC status
# - Image pull failure → Verify image is loaded in Minikube

# Solution: Increase Minikube resources
minikube delete
minikube start --memory=16384 --cpus=8
```

#### Issue 2: Permission Denied on PVCs

```bash
# Pods run as UID 1000, fix PVC permissions
kubectl apply -f k8s/fix-pvc-permissions.yaml

# Verify job completed
kubectl logs job/fix-pvc-permissions -n punjab-analysis
```

#### Issue 3: DAG Not Showing in UI

```bash
# Update ConfigMap
kubectl delete configmap punjab-kubernetes-dag -n punjab-analysis
kubectl create configmap punjab-kubernetes-dag \
  --from-file=airflow/dags/punjab_analysis_kubernetes_dag.py \
  -n punjab-analysis

# Restart scheduler
kubectl delete pod -l component=scheduler -n punjab-analysis
kubectl wait --for=condition=ready pod -l component=scheduler -n punjab-analysis --timeout=60s
```

#### Issue 4: Database Connection Failed

```bash
# Check PostgreSQL is running
kubectl get pods -n punjab-analysis | grep postgres

# Check service
kubectl get svc airflow-postgres -n punjab-analysis

# Test connection
kubectl run -it --rm debug --image=postgres:13 --restart=Never -n punjab-analysis -- \
  psql -h airflow-postgres -U airflow -d airflow -p 5436
```

#### Issue 5: Task Failing

```bash
# Get task logs
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow tasks logs punjab_analysis_kubernetes_pipeline extract_data <run_id>

# Check pod logs directly
kubectl logs -n punjab-analysis extract-all-tenants-xxxxx
```

---

## 🌐 API Reference

### Airflow REST API

Base URL: `http://192.168.49.2:30082/api/v1`

Authentication: Basic Auth (`admin:admin`)

#### List DAGs

```bash
curl -X GET "http://192.168.49.2:30082/api/v1/dags" \
  --user admin:admin
```

#### Trigger DAG

```bash
curl -X POST "http://192.168.49.2:30082/api/v1/dags/punjab_analysis_kubernetes_pipeline/dagRuns" \
  --user admin:admin \
  -H "Content-Type: application/json" \
  -d '{
    "conf": {
      "tenant_ids": ["pb.adampur", "pb.amloh"]
    }
  }'
```

#### Get DAG Runs

```bash
curl -X GET "http://192.168.49.2:30082/api/v1/dags/punjab_analysis_kubernetes_pipeline/dagRuns" \
  --user admin:admin
```

---

## 📖 Commands Cheat Sheet

### Cluster Management

```bash
# Start Minikube
minikube start --memory=8192 --cpus=4

# Stop Minikube
minikube stop

# Access Airflow UI
minikube service airflow-webserver -n punjab-analysis
```

### Pod Operations

```bash
# List all pods
kubectl get pods -n punjab-analysis

# Watch pods in real-time
kubectl get pods -n punjab-analysis -w

# View logs
kubectl logs <pod-name> -n punjab-analysis --tail=100 -f

# Execute command in pod
kubectl exec -it <pod-name> -n punjab-analysis -- bash
```

### DAG Operations

```bash
# Trigger single tenant
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow dags trigger punjab_analysis_kubernetes_pipeline \
  -c '{"tenant_id": "pb.amloh"}'

# Trigger multiple tenants
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow dags trigger punjab_analysis_kubernetes_pipeline \
  -c '{"tenant_ids": ["pb.adampur", "pb.amloh"]}'

# List DAG runs
kubectl exec -n punjab-analysis deployment/airflow-webserver -- \
  airflow dags list-runs -d punjab_analysis_kubernetes_pipeline
```

### Configuration Updates

```bash
# Update DAG
kubectl delete configmap punjab-kubernetes-dag -n punjab-analysis
kubectl create configmap punjab-kubernetes-dag \
  --from-file=airflow/dags/punjab_analysis_kubernetes_dag.py \
  -n punjab-analysis
kubectl delete pod -l component=scheduler -n punjab-analysis

# Update Docker image
cd src
docker build -t punjab-analysis:v3.0.0 .
minikube image load punjab-analysis:v3.0.0
kubectl rollout restart deployment/airflow-scheduler -n punjab-analysis
```

---

## 🔄 Migration from Docker

### Key Differences

| Aspect | Docker Compose | Kubernetes |
|--------|---------------|------------|
| **Architecture** | Single machine | Distributed cluster |
| **Scalability** | Manual | Auto-scaling capable |
| **DAG Tasks** | 12+ tasks (per-tenant) | 5 tasks (unified) |
| **Resource Management** | Manual limits | Dynamic allocation |
| **Storage** | Local volumes | Persistent Volume Claims |
| **Networking** | Docker networks | Kubernetes services |

### Migration Steps

1. Export data from Docker Compose PostgreSQL
2. Stop Docker Compose: `docker-compose down`
3. Follow [Quick Start](#quick-start) to deploy Kubernetes
4. Import data to Kubernetes PostgreSQL
5. Verify pipeline runs successfully

---

## 📝 Project Structure

```
airflow-punjab-analysis/
├── src/                          # Application source code
│   ├── main.py                   # Entry point
│   ├── extract_data.py           # Data extraction logic
│   ├── analyze_data.py           # Data analysis logic
│   ├── process_all_tenants.py    # Multi-tenant processor
│   ├── cleanup_data.py           # Cleanup logic
│   ├── config_loader.py          # Configuration loader
│   ├── requirements.txt          # Python dependencies
│   └── Dockerfile                # Docker image definition
├── airflow/
│   └── dags/
│       └── punjab_analysis_kubernetes_dag.py  # Kubernetes DAG
├── k8s/                          # Kubernetes manifests
│   ├── postgres-deployment.yaml
│   ├── airflow-scheduler-deployment.yaml
│   ├── airflow-webserver-deployment.yaml
│   ├── punjab-pvcs.yaml
│   └── fix-pvc-permissions.yaml
├── secrets/
│   └── db_config.yaml            # Database configuration
└── README.md                     # This file
```

---

## 🎉 Acknowledgments

- Apache Airflow for workflow orchestration
- Kubernetes for container orchestration
- Minikube for local Kubernetes development
- PostgreSQL for reliable data storage

---

**Version**: 3.0.0  
**Last Updated**: October 2025  
**Status**: Production Ready ✅
