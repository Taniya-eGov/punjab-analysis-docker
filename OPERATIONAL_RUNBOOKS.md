# Punjab Analysis System - Operational Runbooks

## 📋 Table of Contents

1. [Daily Operations](#daily-operations)
2. [Deployment Procedures](#deployment-procedures)
3. [Maintenance Tasks](#maintenance-tasks)
4. [Backup & Recovery](#backup--recovery)
5. [Scaling Operations](#scaling-operations)
6. [Security Operations](#security-operations)
7. [Emergency Procedures](#emergency-procedures)

---

## 🔄 Daily Operations

### 1. System Health Check

#### Morning Health Check (5 minutes)

```bash
#!/bin/bash
# Daily health check script

echo "🔍 Punjab Analysis System - Daily Health Check"
echo "=============================================="

# Check cluster status
echo "📊 Kubernetes Cluster Status:"
kubectl get nodes
kubectl get pods -n punjab-analysis

# Check Airflow components
echo "🌪️  Airflow Status:"
kubectl get pods -l component=webserver -n punjab-analysis
kubectl get pods -l component=scheduler -n punjab-analysis

# Check database
echo "🗄️  Database Status:"
kubectl get pods -l app=postgres -n punjab-analysis

# Check storage
echo "💾 Storage Status:"
kubectl get pvc -n punjab-analysis

# Check recent DAG runs
echo "📈 Recent DAG Runs:"
kubectl logs -l component=scheduler -n punjab-analysis --tail=20

echo "✅ Health check completed"
```

#### Airflow UI Access Check

```bash
# Get Airflow URL
MINIKUBE_IP=$(minikube ip)
NODEPORT=$(kubectl get svc airflow-webserver -n punjab-analysis -o jsonpath='{.spec.ports[0].nodePort}')
echo "Airflow UI: http://$MINIKUBE_IP:$NODEPORT"
echo "Username: admin, Password: admin123"
```

### 2. DAG Monitoring

#### Check DAG Status

```bash
# List all DAGs
curl -u admin:admin123 http://$(minikube ip):30082/api/v1/dags

# Check specific DAG runs
curl -u admin:admin123 http://$(minikube ip):30082/api/v1/dags/punjab_analysis_kubernetes_pipeline/dagRuns

# Get latest DAG run status
curl -u admin:admin123 http://$(minikube ip):30082/api/v1/dags/punjab_analysis_kubernetes_pipeline/dagRuns/latest
```

#### Monitor Task Execution

```bash
# Watch pod creation for tasks
kubectl get pods -n punjab-analysis -w

# Check task logs
kubectl logs -l app=punjab-analysis -n punjab-analysis --tail=50

# Monitor resource usage
kubectl top pods -n punjab-analysis
```

### 3. Data Processing Verification

#### Check Output Files

```bash
# Create debug pod to check outputs
kubectl run -it --rm output-checker --image=busybox --restart=Never -n punjab-analysis -- sh

# Inside the pod:
ls -la /output/
ls -la /data/
cat /output/analysis_summary.csv
```

#### Verify Data Quality

```bash
# Check file sizes and counts
kubectl exec -it output-checker -n punjab-analysis -- sh -c "
echo 'Output files:'
ls -lh /output/
echo 'Data files:'
ls -lh /data/
echo 'File counts:'
find /output -name '*.csv' | wc -l
find /data -name '*.csv' | wc -l
"
```

---

## 🚀 Deployment Procedures

### 1. Initial Deployment

#### Prerequisites Check

```bash
#!/bin/bash
# Pre-deployment checklist

echo "🔍 Pre-deployment Checklist"
echo "=========================="

# Check Minikube status
if ! minikube status > /dev/null 2>&1; then
    echo "❌ Minikube is not running. Start with: minikube start"
    exit 1
fi
echo "✅ Minikube is running"

# Check kubectl access
if ! kubectl cluster-info > /dev/null 2>&1; then
    echo "❌ kubectl cannot access cluster"
    exit 1
fi
echo "✅ kubectl access confirmed"

# Check Docker image
if ! docker images | grep punjab-analysis > /dev/null 2>&1; then
    echo "❌ Docker image not found. Build with: cd src && ./build.sh"
    exit 1
fi
echo "✅ Docker image exists"

# Check required files
REQUIRED_FILES=(
    "k8s/01-namespace.yaml"
    "k8s/05-persistent-volumes.yaml"
    "k8s/06-persistent-volume-claims.yaml"
    "k8s/07-airflow-deployment.yaml"
    "k8s/secrets/db_config.yaml"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Required file missing: $file"
        exit 1
    fi
done
echo "✅ All required files present"

echo "🎉 Pre-deployment checks passed"
```

#### Step-by-Step Deployment

```bash
#!/bin/bash
# Complete deployment procedure

echo "🚀 Starting Punjab Analysis System Deployment"
echo "============================================="

# Step 1: Build and load image
echo "📦 Step 1: Building and loading Docker image..."
cd src
./build.sh
minikube image load punjab-analysis:v3.0.0
cd ..

# Step 2: Create namespace
echo "🏗️  Step 2: Creating namespace..."
kubectl apply -f k8s/01-namespace.yaml

# Step 3: Deploy storage
echo "💾 Step 3: Deploying storage..."
kubectl apply -f k8s/05-persistent-volumes.yaml
kubectl apply -f k8s/06-persistent-volume-claims.yaml

# Step 4: Deploy PostgreSQL
echo "🗄️  Step 4: Deploying PostgreSQL..."
kubectl apply -f k8s/postgres-deployment.yaml

# Step 5: Wait for PostgreSQL
echo "⏳ Step 5: Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres -n punjab-analysis --timeout=300s

# Step 6: Deploy Airflow
echo "🌪️  Step 6: Deploying Airflow..."
kubectl apply -f k8s/07-airflow-deployment.yaml
kubectl apply -f k8s/08-airflow-service.yaml
kubectl apply -f k8s/09-rbac.yaml

# Step 7: Deploy DAG
echo "📋 Step 7: Deploying DAG..."
kubectl apply -f k8s/10-dag-configmap.yaml
kubectl apply -f k8s/11-pod-template.yaml
kubectl apply -f k8s/12-pod-template-configmap.yaml

# Step 8: Wait for Airflow
echo "⏳ Step 8: Waiting for Airflow to be ready..."
kubectl wait --for=condition=ready pod -l component=webserver -n punjab-analysis --timeout=300s
kubectl wait --for=condition=ready pod -l component=scheduler -n punjab-analysis --timeout=300s

# Step 9: Verify deployment
echo "✅ Step 9: Verifying deployment..."
kubectl get pods -n punjab-analysis
kubectl get svc -n punjab-analysis

# Step 10: Get access information
echo "🌐 Step 10: Getting access information..."
MINIKUBE_IP=$(minikube ip)
NODEPORT=$(kubectl get svc airflow-webserver -n punjab-analysis -o jsonpath='{.spec.ports[0].nodePort}')

echo "🎉 Deployment completed successfully!"
echo "====================================="
echo "Airflow UI: http://$MINIKUBE_IP:$NODEPORT"
echo "Username: admin"
echo "Password: admin123"
echo "====================================="
```

### 2. Application Updates

#### Code Update Procedure

```bash
#!/bin/bash
# Update application code

echo "🔄 Updating Punjab Analysis Application"
echo "======================================="

# Step 1: Build new image
echo "📦 Building new Docker image..."
cd src
./build.sh
cd ..

# Step 2: Load into Minikube
echo "⬆️  Loading image into Minikube..."
minikube image load punjab-analysis:v3.0.0

# Step 3: Restart deployments
echo "🔄 Restarting deployments..."
kubectl rollout restart deployment/airflow-webserver -n punjab-analysis
kubectl rollout restart deployment/airflow-scheduler -n punjab-analysis

# Step 4: Wait for rollout
echo "⏳ Waiting for rollout to complete..."
kubectl rollout status deployment/airflow-webserver -n punjab-analysis
kubectl rollout status deployment/airflow-scheduler -n punjab-analysis

# Step 5: Verify update
echo "✅ Verifying update..."
kubectl get pods -n punjab-analysis

echo "🎉 Application update completed!"
```

#### DAG Update Procedure

```bash
#!/bin/bash
# Update DAG configuration

echo "📋 Updating DAG Configuration"
echo "============================="

# Step 1: Update DAG ConfigMap
echo "🔄 Updating DAG ConfigMap..."
kubectl apply -f k8s/10-dag-configmap.yaml

# Step 2: Restart Airflow components
echo "🔄 Restarting Airflow components..."
kubectl rollout restart deployment/airflow-webserver -n punjab-analysis
kubectl rollout restart deployment/airflow-scheduler -n punjab-analysis

# Step 3: Wait for restart
echo "⏳ Waiting for restart..."
kubectl rollout status deployment/airflow-webserver -n punjab-analysis
kubectl rollout status deployment/airflow-scheduler -n punjab-analysis

# Step 4: Verify DAG is loaded
echo "✅ Verifying DAG is loaded..."
sleep 30
curl -u admin:admin123 http://$(minikube ip):30082/api/v1/dags/punjab_analysis_kubernetes_pipeline

echo "🎉 DAG update completed!"
```

---

## 🔧 Maintenance Tasks

### 1. Regular Maintenance

#### Weekly Maintenance (30 minutes)

```bash
#!/bin/bash
# Weekly maintenance tasks

echo "🔧 Weekly Maintenance Tasks"
echo "==========================="

# 1. Clean up old pods
echo "🧹 Cleaning up old pods..."
kubectl delete pods --field-selector=status.phase=Succeeded -n punjab-analysis
kubectl delete pods --field-selector=status.phase=Failed -n punjab-analysis

# 2. Check storage usage
echo "💾 Checking storage usage..."
kubectl get pvc -n punjab-analysis
df -h

# 3. Review logs
echo "📋 Reviewing recent logs..."
kubectl logs -l component=scheduler -n punjab-analysis --tail=100 | grep ERROR

# 4. Check resource usage
echo "📊 Checking resource usage..."
kubectl top pods -n punjab-analysis
kubectl top nodes

# 5. Backup configuration
echo "💾 Backing up configuration..."
kubectl get configmap punjab-kubernetes-dag -n punjab-analysis -o yaml > backup/dag-configmap-$(date +%Y%m%d).yaml
kubectl get secret -n punjab-analysis -o yaml > backup/secrets-$(date +%Y%m%d).yaml

echo "✅ Weekly maintenance completed"
```

#### Monthly Maintenance (2 hours)

```bash
#!/bin/bash
# Monthly maintenance tasks

echo "🔧 Monthly Maintenance Tasks"
echo "============================"

# 1. Update base images
echo "🔄 Checking for image updates..."
docker pull apache/airflow:2.5.3
docker pull postgres:13

# 2. Review and rotate logs
echo "📋 Reviewing and rotating logs..."
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- find /opt/airflow/logs -name "*.log" -mtime +30 -delete

# 3. Database maintenance
echo "🗄️  Database maintenance..."
kubectl exec -it $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- psql -U postgres -d airflowdb -c "VACUUM ANALYZE;"

# 4. Security review
echo "🔒 Security review..."
kubectl get secrets -n punjab-analysis
kubectl get serviceaccounts -n punjab-analysis
kubectl get roles -n punjab-analysis

# 5. Performance analysis
echo "📊 Performance analysis..."
kubectl top pods -n punjab-analysis --sort-by=memory
kubectl top pods -n punjab-analysis --sort-by=cpu

echo "✅ Monthly maintenance completed"
```

### 2. Storage Management

#### PVC Maintenance

```bash
#!/bin/bash
# PVC maintenance tasks

echo "💾 PVC Maintenance Tasks"
echo "========================"

# Check PVC status
echo "📊 PVC Status:"
kubectl get pvc -n punjab-analysis

# Check storage usage
echo "📈 Storage Usage:"
kubectl exec -it $(kubectl get pods -l component=webserver -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- df -h /opt/airflow/logs

# Clean up old data
echo "🧹 Cleaning up old data..."
kubectl run -it --rm cleanup-pod --image=busybox --restart=Never -n punjab-analysis -- sh -c "
echo 'Cleaning up data older than 7 days...'
find /data -name '*.csv' -mtime +7 -delete
find /output -name '*.csv' -mtime +7 -delete
echo 'Cleanup completed'
"

# Resize PVC if needed
echo "📏 To resize PVC (if needed):"
echo "kubectl patch pvc punjab-data-pvc -n punjab-analysis -p '{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"20Gi\"}}}}'"

echo "✅ PVC maintenance completed"
```

---

## 💾 Backup & Recovery

### 1. Backup Procedures

#### Daily Backup

```bash
#!/bin/bash
# Daily backup procedure

echo "💾 Daily Backup Procedure"
echo "========================"

BACKUP_DIR="/backup/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# 1. Backup database
echo "🗄️  Backing up database..."
kubectl exec -it $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- pg_dump -U postgres airflowdb > $BACKUP_DIR/database_backup.sql

# 2. Backup configuration
echo "⚙️  Backing up configuration..."
kubectl get configmap punjab-kubernetes-dag -n punjab-analysis -o yaml > $BACKUP_DIR/dag-configmap.yaml
kubectl get secret -n punjab-analysis -o yaml > $BACKUP_DIR/secrets.yaml

# 3. Backup PVC data
echo "💾 Backing up PVC data..."
kubectl run -it --rm backup-pod --image=busybox --restart=Never -n punjab-analysis -- sh -c "
tar -czf /tmp/output-backup.tar.gz /output/
tar -czf /tmp/data-backup.tar.gz /data/
"

kubectl cp punjab-analysis/backup-pod:/tmp/output-backup.tar.gz $BACKUP_DIR/
kubectl cp punjab-analysis/backup-pod:/tmp/data-backup.tar.gz $BACKUP_DIR/

# 4. Cleanup backup pod
kubectl delete pod backup-pod -n punjab-analysis

echo "✅ Daily backup completed: $BACKUP_DIR"
```

#### Weekly Full Backup

```bash
#!/bin/bash
# Weekly full backup procedure

echo "💾 Weekly Full Backup Procedure"
echo "==============================="

BACKUP_DIR="/backup/weekly/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# 1. Full database backup
echo "🗄️  Full database backup..."
kubectl exec -it $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- pg_dump -U postgres -Fc airflowdb > $BACKUP_DIR/database_full_backup.dump

# 2. All configurations
echo "⚙️  All configurations..."
kubectl get all -n punjab-analysis -o yaml > $BACKUP_DIR/all-resources.yaml

# 3. Complete PVC backup
echo "💾 Complete PVC backup..."
kubectl run -it --rm full-backup-pod --image=busybox --restart=Never -n punjab-analysis -- sh -c "
tar -czf /tmp/complete-backup.tar.gz /data/ /output/ /app/secrets/
"

kubectl cp punjab-analysis/full-backup-pod:/tmp/complete-backup.tar.gz $BACKUP_DIR/
kubectl delete pod full-backup-pod -n punjab-analysis

# 4. Compress backup
echo "📦 Compressing backup..."
tar -czf $BACKUP_DIR/weekly-backup-$(date +%Y%m%d).tar.gz -C $BACKUP_DIR .

echo "✅ Weekly backup completed: $BACKUP_DIR"
```

### 2. Recovery Procedures

#### Database Recovery

```bash
#!/bin/bash
# Database recovery procedure

echo "🔄 Database Recovery Procedure"
echo "=============================="

BACKUP_FILE=$1
if [ -z "$BACKUP_FILE" ]; then
    echo "Usage: $0 <backup_file>"
    exit 1
fi

# 1. Stop Airflow
echo "⏹️  Stopping Airflow..."
kubectl scale deployment airflow-webserver --replicas=0 -n punjab-analysis
kubectl scale deployment airflow-scheduler --replicas=0 -n punjab-analysis

# 2. Restore database
echo "🔄 Restoring database..."
kubectl exec -i $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- psql -U postgres -d airflowdb < $BACKUP_FILE

# 3. Restart Airflow
echo "▶️  Restarting Airflow..."
kubectl scale deployment airflow-webserver --replicas=1 -n punjab-analysis
kubectl scale deployment airflow-scheduler --replicas=1 -n punjab-analysis

# 4. Wait for restart
echo "⏳ Waiting for restart..."
kubectl wait --for=condition=ready pod -l component=webserver -n punjab-analysis --timeout=300s
kubectl wait --for=condition=ready pod -l component=scheduler -n punjab-analysis --timeout=300s

echo "✅ Database recovery completed"
```

#### Full System Recovery

```bash
#!/bin/bash
# Full system recovery procedure

echo "🔄 Full System Recovery Procedure"
echo "================================="

BACKUP_DIR=$1
if [ -z "$BACKUP_DIR" ]; then
    echo "Usage: $0 <backup_directory>"
    exit 1
fi

# 1. Stop all services
echo "⏹️  Stopping all services..."
kubectl delete deployment --all -n punjab-analysis
kubectl delete service --all -n punjab-analysis

# 2. Restore configurations
echo "🔄 Restoring configurations..."
kubectl apply -f $BACKUP_DIR/dag-configmap.yaml
kubectl apply -f $BACKUP_DIR/secrets.yaml

# 3. Restore database
echo "🗄️  Restoring database..."
kubectl apply -f k8s/postgres-deployment.yaml
kubectl wait --for=condition=ready pod -l app=postgres -n punjab-analysis --timeout=300s
kubectl exec -i $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- psql -U postgres -d airflowdb < $BACKUP_DIR/database_backup.sql

# 4. Restore PVC data
echo "💾 Restoring PVC data..."
kubectl run -it --rm restore-pod --image=busybox --restart=Never -n punjab-analysis -- sh -c "
tar -xzf /tmp/complete-backup.tar.gz -C /
"

kubectl cp $BACKUP_DIR/complete-backup.tar.gz punjab-analysis/restore-pod:/tmp/
kubectl delete pod restore-pod -n punjab-analysis

# 5. Restart services
echo "▶️  Restarting services..."
kubectl apply -f k8s/07-airflow-deployment.yaml
kubectl apply -f k8s/08-airflow-service.yaml

echo "✅ Full system recovery completed"
```

---

## 📈 Scaling Operations

### 1. Horizontal Scaling

#### Scale Airflow Components

```bash
#!/bin/bash
# Scale Airflow components

echo "📈 Scaling Airflow Components"
echo "============================="

# Scale webserver
echo "🌐 Scaling webserver..."
kubectl scale deployment airflow-webserver --replicas=2 -n punjab-analysis

# Scale scheduler
echo "⏰ Scaling scheduler..."
kubectl scale deployment airflow-scheduler --replicas=2 -n punjab-analysis

# Verify scaling
echo "✅ Verifying scaling..."
kubectl get pods -n punjab-analysis
kubectl get deployment -n punjab-analysis

echo "🎉 Scaling completed"
```

#### Auto-scaling Configuration

```bash
#!/bin/bash
# Configure auto-scaling

echo "🤖 Configuring Auto-scaling"
echo "==========================="

# Create HPA for webserver
kubectl autoscale deployment airflow-webserver --cpu-percent=70 --min=1 --max=3 -n punjab-analysis

# Create HPA for scheduler
kubectl autoscale deployment airflow-scheduler --cpu-percent=70 --min=1 --max=2 -n punjab-analysis

# Verify HPA
echo "✅ Verifying HPA..."
kubectl get hpa -n punjab-analysis

echo "🎉 Auto-scaling configured"
```

### 2. Vertical Scaling

#### Resource Limits Update

```bash
#!/bin/bash
# Update resource limits

echo "📊 Updating Resource Limits"
echo "==========================="

# Update webserver resources
kubectl patch deployment airflow-webserver -n punjab-analysis -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "webserver",
          "resources": {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "1", "memory": "2Gi"}
          }
        }]
      }
    }
  }
}'

# Update scheduler resources
kubectl patch deployment airflow-scheduler -n punjab-analysis -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "scheduler",
          "resources": {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "1", "memory": "2Gi"}
          }
        }]
      }
    }
  }
}'

echo "✅ Resource limits updated"
```

---

## 🔒 Security Operations

### 1. Security Updates

#### Update Base Images

```bash
#!/bin/bash
# Update base images for security

echo "🔒 Security Update Procedure"
echo "============================"

# Update Airflow image
echo "🌪️  Updating Airflow image..."
docker pull apache/airflow:2.5.3
kubectl set image deployment/airflow-webserver webserver=apache/airflow:2.5.3 -n punjab-analysis
kubectl set image deployment/airflow-scheduler scheduler=apache/airflow:2.5.3 -n punjab-analysis

# Update PostgreSQL image
echo "🗄️  Updating PostgreSQL image..."
docker pull postgres:13
kubectl set image deployment/postgres postgres=postgres:13 -n punjab-analysis

# Update application image
echo "📦 Updating application image..."
cd src
./build.sh
minikube image load punjab-analysis:v3.0.0
cd ..

# Wait for rollout
echo "⏳ Waiting for rollout..."
kubectl rollout status deployment/airflow-webserver -n punjab-analysis
kubectl rollout status deployment/airflow-scheduler -n punjab-analysis
kubectl rollout status deployment/postgres -n punjab-analysis

echo "✅ Security updates completed"
```

#### Rotate Secrets

```bash
#!/bin/bash
# Rotate secrets

echo "🔐 Secret Rotation Procedure"
echo "============================"

# Generate new password
NEW_PASSWORD=$(openssl rand -base64 32)

# Update database password
echo "🔄 Updating database password..."
kubectl patch secret postgres-secret -n punjab-analysis -p "{\"data\":{\"password\":\"$(echo -n $NEW_PASSWORD | base64)\"}}"

# Update config file
echo "📝 Updating config file..."
kubectl run -it --rm config-updater --image=busybox --restart=Never -n punjab-analysis -- sh -c "
sed -i 's/password: .*/password: $NEW_PASSWORD/' /app/secrets/db_config.yaml
"

# Restart services
echo "🔄 Restarting services..."
kubectl rollout restart deployment/airflow-webserver -n punjab-analysis
kubectl rollout restart deployment/airflow-scheduler -n punjab-analysis

echo "✅ Secret rotation completed"
```

### 2. Security Audits

#### RBAC Audit

```bash
#!/bin/bash
# RBAC security audit

echo "🔍 RBAC Security Audit"
echo "======================"

# Check service accounts
echo "👤 Service Accounts:"
kubectl get serviceaccounts -n punjab-analysis

# Check roles
echo "🎭 Roles:"
kubectl get roles -n punjab-analysis

# Check role bindings
echo "🔗 Role Bindings:"
kubectl get rolebindings -n punjab-analysis

# Check cluster roles
echo "🌐 Cluster Roles:"
kubectl get clusterroles | grep punjab

# Check cluster role bindings
echo "🔗 Cluster Role Bindings:"
kubectl get clusterrolebindings | grep punjab

echo "✅ RBAC audit completed"
```

#### Network Security Audit

```bash
#!/bin/bash
# Network security audit

echo "🌐 Network Security Audit"
echo "========================="

# Check services
echo "🔌 Services:"
kubectl get svc -n punjab-analysis

# Check network policies
echo "🛡️  Network Policies:"
kubectl get networkpolicies -n punjab-analysis

# Check ingress
echo "🚪 Ingress:"
kubectl get ingress -n punjab-analysis

# Check pod security contexts
echo "🔒 Pod Security Contexts:"
kubectl get pods -n punjab-analysis -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.securityContext}{"\n"}{end}'

echo "✅ Network security audit completed"
```

---

## 🚨 Emergency Procedures

### 1. System Recovery

#### Complete System Restart

```bash
#!/bin/bash
# Emergency system restart

echo "🚨 Emergency System Restart"
echo "==========================="

# 1. Stop all deployments
echo "⏹️  Stopping all deployments..."
kubectl scale deployment --all --replicas=0 -n punjab-analysis

# 2. Wait for pods to terminate
echo "⏳ Waiting for pods to terminate..."
kubectl wait --for=delete pod --all -n punjab-analysis --timeout=60s

# 3. Restart deployments
echo "▶️  Restarting deployments..."
kubectl scale deployment postgres --replicas=1 -n punjab-analysis
kubectl wait --for=condition=ready pod -l app=postgres -n punjab-analysis --timeout=300s

kubectl scale deployment airflow-webserver --replicas=1 -n punjab-analysis
kubectl scale deployment airflow-scheduler --replicas=1 -n punjab-analysis

# 4. Wait for Airflow
echo "⏳ Waiting for Airflow..."
kubectl wait --for=condition=ready pod -l component=webserver -n punjab-analysis --timeout=300s
kubectl wait --for=condition=ready pod -l component=scheduler -n punjab-analysis --timeout=300s

# 5. Verify system
echo "✅ Verifying system..."
kubectl get pods -n punjab-analysis
kubectl get svc -n punjab-analysis

echo "🎉 Emergency restart completed"
```

#### Database Recovery

```bash
#!/bin/bash
# Emergency database recovery

echo "🚨 Emergency Database Recovery"
echo "=============================="

# 1. Check database status
echo "🔍 Checking database status..."
kubectl get pods -l app=postgres -n punjab-analysis

# 2. Check database logs
echo "📋 Checking database logs..."
kubectl logs -l app=postgres -n punjab-analysis --tail=50

# 3. Restart database if needed
echo "🔄 Restarting database..."
kubectl rollout restart deployment/postgres -n punjab-analysis

# 4. Wait for database
echo "⏳ Waiting for database..."
kubectl wait --for=condition=ready pod -l app=postgres -n punjab-analysis --timeout=300s

# 5. Test database connection
echo "🔌 Testing database connection..."
kubectl exec -it $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- psql -U postgres -d airflowdb -c "SELECT 1;"

echo "✅ Database recovery completed"
```

### 2. Data Recovery

#### Emergency Data Recovery

```bash
#!/bin/bash
# Emergency data recovery

echo "🚨 Emergency Data Recovery"
echo "=========================="

# 1. Check PVC status
echo "💾 Checking PVC status..."
kubectl get pvc -n punjab-analysis

# 2. Check pod mounts
echo "🔗 Checking pod mounts..."
kubectl describe pod -l component=webserver -n punjab-analysis | grep -A 10 "Mounts:"

# 3. Create recovery pod
echo "🔄 Creating recovery pod..."
kubectl run -it --rm recovery-pod --image=busybox --restart=Never -n punjab-analysis -- sh -c "
echo 'Checking mounted volumes...'
ls -la /data/
ls -la /output/
ls -la /app/secrets/
echo 'Data recovery check completed'
"

# 4. Restore from backup if needed
echo "💾 To restore from backup:"
echo "kubectl cp backup/data-backup.tar.gz punjab-analysis/recovery-pod:/tmp/"
echo "kubectl exec -it recovery-pod -n punjab-analysis -- tar -xzf /tmp/data-backup.tar.gz -C /"

echo "✅ Data recovery check completed"
```

---

## 📞 Support Contacts

### Emergency Contacts

- **System Administrator**: [Your Contact]
- **Database Administrator**: [DB Admin Contact]
- **Kubernetes Administrator**: [K8s Admin Contact]

### Escalation Procedures

1. **Level 1**: Check logs and basic troubleshooting
2. **Level 2**: Restart services and check configurations
3. **Level 3**: Contact system administrator
4. **Level 4**: Contact database/kubernetes administrator

### Documentation References

- **Technical Documentation**: `TECHNICAL_DOCUMENTATION.md`
- **Troubleshooting Guide**: `TROUBLESHOOTING_GUIDE.md`
- **User Training**: `USER_TRAINING_MATERIALS.md`

---

_This operational runbook provides step-by-step procedures for all common operational tasks. Always test procedures in a non-production environment first._
