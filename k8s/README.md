# Punjab Analysis System - Kubernetes Migration

This directory contains all the Kubernetes manifests and deployment scripts for migrating the Punjab Analysis System from Docker to Kubernetes.

## 🚀 Quick Start

### Prerequisites
- Kubernetes cluster (minikube recommended for local development)
- Docker image: `punjab-analysis:v3.0.0`
- kubectl configured to access the cluster

### 1. Build and Load Docker Image
```bash
# Build the image (if not already built)
cd ../src/
./build.sh

# For minikube, load the image into the cluster
minikube image load punjab-analysis:v3.0.0
```

### 2. Deploy to Kubernetes
```bash
cd k8s/
./deploy.sh
```

### 3. Access Airflow
```bash
# Get the access URL
minikube ip  # Get minikube IP
kubectl get svc airflow-webserver -n punjab-analysis  # Get NodePort

# Access: http://<minikube-ip>:<node-port>
# Username: admin, Password: admin123
```

## 📁 File Structure

```
k8s/
├── 01-namespace.yaml                    # Kubernetes namespace
├── 02-configmap.yaml                   # Application configuration
├── 03-secrets.yaml                     # Database credentials
├── 04-postgres-external-service.yaml   # External PostgreSQL service
├── 05-persistent-volumes.yaml          # Storage volumes
├── 06-persistent-volume-claims.yaml    # Storage claims
├── 07-airflow-deployment.yaml          # Airflow webserver & scheduler
├── 08-airflow-service.yaml             # Airflow web service
├── 09-rbac.yaml                        # Service account & permissions
├── punjab_analysis_kubernetes_dag.py   # Kubernetes-based DAG
├── deploy.sh                           # Deployment script
└── README.md                           # This file
```

## 🏗️ Architecture

### Current Docker Architecture
```
Docker Compose
├── Airflow (DockerOperator)
├── PostgreSQL (Container)
└── Punjab Analysis (Docker containers)
```

### New Kubernetes Architecture
```
Kubernetes Cluster
├── Airflow (KubernetesExecutor)
│   ├── Webserver (Deployment)
│   └── Scheduler (Deployment)
├── External PostgreSQL (Service → Docker)
├── Punjab Analysis Pods (KubernetesPodOperator)
│   ├── Extract Pods (Parallel processing)
│   └── Analysis Pods (Resource-optimized)
└── Persistent Storage (PV/PVC)
```

## 🔄 Migration Benefits

### Scalability
- **Before**: Sequential processing (1 tenant at a time)
- **After**: Parallel processing (multiple tenants simultaneously)

### Resource Management
- **Before**: Fixed host resources
- **After**: Dynamic resource allocation per pod

### High Availability
- **Before**: Single point of failure
- **After**: Multi-pod resilience with auto-restart

### Monitoring
- **Before**: Docker logs only
- **After**: Kubernetes metrics, pod logs, events

## 📊 Resource Configuration

### Extract Pods
```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "2000m"
```

### Analysis Pods
```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "4000m"
```

### Cleanup Pods
```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "100m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

## 🗄️ Storage Configuration

### Persistent Volumes
- **Data Volume**: 10Gi (ReadWriteMany) - Raw extracted data
- **Output Volume**: 5Gi (ReadWriteMany) - Analysis results
- **Secrets Volume**: 1Gi (ReadOnlyMany) - Database credentials

### Volume Mounts
```yaml
volumeMounts:
  - name: data-storage
    mountPath: /data
  - name: output-storage
    mountPath: /output
  - name: secrets-storage
    mountPath: /run/secrets
    readOnly: true
```

## 🔐 Security Configuration

### Service Account
- **Name**: `airflow-worker`
- **Permissions**: Pod management, ConfigMaps, Secrets access
- **Security Context**: Non-root user (1000:1000)

### RBAC
- **Namespace-scoped**: Limited to `punjab-analysis` namespace
- **Minimal permissions**: Only required operations allowed

## 🔍 Monitoring & Debugging

### Useful Commands
```bash
# Watch pods in real-time
kubectl get pods -n punjab-analysis -w

# Check Airflow logs
kubectl logs -f deployment/airflow-scheduler -n punjab-analysis

# Describe a specific pod
kubectl describe pod <pod-name> -n punjab-analysis

# Check persistent volume claims
kubectl get pvc -n punjab-analysis

# Check events
kubectl get events -n punjab-analysis --sort-by='.lastTimestamp'
```

### Common Issues

#### Pod Stuck in Pending
```bash
# Check PVC status
kubectl get pvc -n punjab-analysis

# Check node resources
kubectl describe nodes

# Check pod events
kubectl describe pod <pod-name> -n punjab-analysis
```

#### Image Pull Errors
```bash
# For minikube, ensure image is loaded
minikube image ls | grep punjab-analysis

# If missing, load it
minikube image load punjab-analysis:v3.0.0
```

## 🔄 Migration Path

### Phase 1: External PostgreSQL (Current)
- Keep Docker PostgreSQL
- Migrate only workloads to Kubernetes
- External service points to host PostgreSQL

### Phase 2: Full Kubernetes (Future)
- Migrate PostgreSQL to StatefulSet
- All components in Kubernetes
- Complete cloud-native architecture

### Migration Command
```bash
# Switch from external to StatefulSet PostgreSQL
kubectl apply -f postgres-statefulset.yaml  # Future file
kubectl patch service airflow-postgres --type='merge' -p='{"spec":{"type":"ClusterIP"}}'
```

## 📈 Performance Improvements

### Parallel Processing
- **Docker**: 3 tenants × 2 tasks = 6 sequential operations
- **Kubernetes**: 3 tenants × 2 tasks = 6 parallel operations
- **Speed improvement**: ~3x faster with adequate cluster resources

### Resource Efficiency
- **Extract**: Light CPU/memory for data extraction
- **Analysis**: Heavy CPU/memory for complex analysis
- **Cleanup**: Minimal resources for maintenance

### Auto-scaling Potential
```yaml
# Future: Horizontal Pod Autoscaler
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: punjab-analysis-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: punjab-analysis-workers
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

## 🎯 Next Steps

1. **Deploy and Test**: Run the deployment script and validate functionality
2. **Monitor Performance**: Compare execution times vs Docker approach
3. **Scale Testing**: Test with larger tenant lists
4. **StatefulSet Migration**: Plan PostgreSQL migration to Kubernetes
5. **Production Readiness**: Add monitoring, alerting, and backup strategies