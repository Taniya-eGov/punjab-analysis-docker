# Punjab Analysis System - Troubleshooting Guide

## 📋 Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Common Issues](#common-issues)
3. [Component-Specific Troubleshooting](#component-specific-troubleshooting)
4. [Error Messages & Solutions](#error-messages--solutions)
5. [Performance Issues](#performance-issues)
6. [Network Issues](#network-issues)
7. [Storage Issues](#storage-issues)
8. [Recovery Procedures](#recovery-procedures)

---

## 🔍 Quick Diagnostics

### System Health Check

```bash
#!/bin/bash
# Quick system health check

echo "🔍 Punjab Analysis System - Quick Health Check"
echo "=============================================="

# Check cluster status
echo "📊 Kubernetes Cluster:"
kubectl cluster-info
kubectl get nodes

# Check namespace
echo "🏷️  Namespace Status:"
kubectl get ns punjab-analysis

# Check all pods
echo "🐳 Pod Status:"
kubectl get pods -n punjab-analysis

# Check services
echo "🔌 Service Status:"
kubectl get svc -n punjab-analysis

# Check PVCs
echo "💾 Storage Status:"
kubectl get pvc -n punjab-analysis

# Check recent events
echo "📋 Recent Events:"
kubectl get events -n punjab-analysis --sort-by='.lastTimestamp' | tail -10

echo "✅ Health check completed"
```

### Quick Fixes

```bash
# Restart all deployments
kubectl rollout restart deployment --all -n punjab-analysis

# Check pod logs
kubectl logs -l component=webserver -n punjab-analysis --tail=50
kubectl logs -l component=scheduler -n punjab-analysis --tail=50

# Check resource usage
kubectl top pods -n punjab-analysis
kubectl top nodes
```

---

## 🚨 Common Issues

### 1. Pods Not Starting

#### Symptoms

- Pods stuck in `Pending` or `ContainerCreating` state
- Pods showing `ImagePullBackOff` or `ErrImagePull` errors

#### Diagnosis

```bash
# Check pod status
kubectl get pods -n punjab-analysis

# Check pod details
kubectl describe pod <pod-name> -n punjab-analysis

# Check events
kubectl get events -n punjab-analysis --sort-by='.lastTimestamp'
```

#### Solutions

##### Image Pull Issues

```bash
# Check if image exists in Minikube
minikube image ls | grep punjab-analysis

# Load image if missing
minikube image load punjab-analysis:v3.0.0

# Check image in Docker
docker images | grep punjab-analysis
```

##### Resource Issues

```bash
# Check node resources
kubectl describe nodes

# Check resource requests/limits
kubectl describe pod <pod-name> -n punjab-analysis | grep -A 10 "Requests:"
```

##### PVC Issues

```bash
# Check PVC status
kubectl get pvc -n punjab-analysis

# Check PV status
kubectl get pv

# Check storage class
kubectl get storageclass
```

### 2. Airflow UI Not Accessible

#### Symptoms

- Cannot access Airflow UI at http://192.168.49.2:30082
- Connection timeout or refused errors

#### Diagnosis

```bash
# Check service status
kubectl get svc airflow-webserver -n punjab-analysis

# Check pod status
kubectl get pods -l component=webserver -n punjab-analysis

# Check NodePort
kubectl get svc airflow-webserver -n punjab-analysis -o jsonpath='{.spec.ports[0].nodePort}'

# Test connectivity
curl -I http://$(minikube ip):30082
```

#### Solutions

##### Service Issues

```bash
# Restart webserver
kubectl rollout restart deployment/airflow-webserver -n punjab-analysis

# Check service endpoints
kubectl get endpoints airflow-webserver -n punjab-analysis

# Verify NodePort
kubectl patch svc airflow-webserver -n punjab-analysis -p '{"spec":{"type":"NodePort","ports":[{"port":8080,"targetPort":8080,"nodePort":30082}]}}'
```

##### Network Issues

```bash
# Check Minikube IP
minikube ip

# Check if Minikube is running
minikube status

# Restart Minikube if needed
minikube stop
minikube start
```

### 3. DAG Not Appearing

#### Symptoms

- DAG not visible in Airflow UI
- DAG parsing errors in logs

#### Diagnosis

```bash
# Check DAG ConfigMap
kubectl get configmap punjab-kubernetes-dag -n punjab-analysis

# Check scheduler logs
kubectl logs -l component=scheduler -n punjab-analysis --tail=100 | grep -i "dag"

# Check DAG file
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- ls -la /opt/airflow/dags/
```

#### Solutions

##### ConfigMap Issues

```bash
# Reapply DAG ConfigMap
kubectl apply -f k8s/10-dag-configmap.yaml

# Restart scheduler
kubectl rollout restart deployment/airflow-scheduler -n punjab-analysis

# Check DAG parsing
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- python -c "
import sys
sys.path.append('/opt/airflow/dags')
try:
    import punjab_analysis_kubernetes_dag
    print('DAG imported successfully')
except Exception as e:
    print(f'DAG import failed: {e}')
"
```

##### Syntax Errors

```bash
# Validate DAG syntax
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- python -m py_compile /opt/airflow/dags/punjab_analysis_kubernetes_dag.py
```

### 4. Database Connection Issues

#### Symptoms

- Tasks failing with database connection errors
- "Connection refused" or "Authentication failed" errors

#### Diagnosis

```bash
# Check PostgreSQL pod
kubectl get pods -l app=postgres -n punjab-analysis

# Check PostgreSQL logs
kubectl logs -l app=postgres -n punjab-analysis --tail=50

# Test database connection
kubectl exec -it $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- psql -U postgres -d airflowdb -c "SELECT 1;"

# Check database service
kubectl get svc -l app=postgres -n punjab-analysis
```

#### Solutions

##### PostgreSQL Issues

```bash
# Restart PostgreSQL
kubectl rollout restart deployment/postgres -n punjab-analysis

# Check database initialization
kubectl exec -it $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- psql -U postgres -c "\l"

# Recreate database if needed
kubectl exec -it $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- psql -U postgres -c "CREATE DATABASE airflowdb;"
```

##### Configuration Issues

```bash
# Check config file
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- cat /app/secrets/db_config.yaml

# Verify database credentials
kubectl get secret postgres-secret -n punjab-analysis -o yaml
```

---

## 🔧 Component-Specific Troubleshooting

### 1. Airflow Webserver

#### Common Issues

##### Webserver Not Starting

```bash
# Check webserver logs
kubectl logs -l component=webserver -n punjab-analysis --tail=100

# Check webserver configuration
kubectl describe pod -l component=webserver -n punjab-analysis

# Check resource limits
kubectl top pod -l component=webserver -n punjab-analysis
```

##### Authentication Issues

```bash
# Check Airflow variables
kubectl exec -it $(kubectl get pods -l component=webserver -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- airflow variables list

# Reset admin password
kubectl exec -it $(kubectl get pods -l component=webserver -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin123
```

### 2. Airflow Scheduler

#### Common Issues

##### Scheduler Not Processing Tasks

```bash
# Check scheduler logs
kubectl logs -l component=scheduler -n punjab-analysis --tail=100

# Check DAG parsing
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- airflow dags list

# Check task queue
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- airflow tasks list punjab_analysis_kubernetes_pipeline
```

##### Memory Issues

```bash
# Check memory usage
kubectl top pod -l component=scheduler -n punjab-analysis

# Check memory limits
kubectl describe pod -l component=scheduler -n punjab-analysis | grep -A 5 "Limits:"

# Increase memory if needed
kubectl patch deployment airflow-scheduler -n punjab-analysis -p '{"spec":{"template":{"spec":{"containers":[{"name":"scheduler","resources":{"limits":{"memory":"4Gi"}}}]}}}}'
```

### 3. Application Pods

#### Common Issues

##### Task Pods Failing

```bash
# Check task pod logs
kubectl logs <task-pod-name> -n punjab-analysis

# Check pod events
kubectl describe pod <task-pod-name> -n punjab-analysis

# Check resource usage
kubectl top pod <task-pod-name> -n punjab-analysis
```

##### Configuration Issues

```bash
# Check environment variables
kubectl exec -it <task-pod-name> -n punjab-analysis -- env | grep -E "(CONFIG_PATH|TENANT_ID|OUTPUT_DIR)"

# Check mounted volumes
kubectl exec -it <task-pod-name> -n punjab-analysis -- ls -la /data/
kubectl exec -it <task-pod-name> -n punjab-analysis -- ls -la /output/
kubectl exec -it <task-pod-name> -n punjab-analysis -- ls -la /app/secrets/
```

---

## ❌ Error Messages & Solutions

### 1. Database Errors

#### "Connection refused"

```bash
# Check PostgreSQL service
kubectl get svc -l app=postgres -n punjab-analysis

# Check PostgreSQL pod
kubectl get pods -l app=postgres -n punjab-analysis

# Restart PostgreSQL
kubectl rollout restart deployment/postgres -n punjab-analysis
```

#### "Authentication failed"

```bash
# Check database credentials
kubectl get secret postgres-secret -n punjab-analysis -o yaml

# Verify config file
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- cat /app/secrets/db_config.yaml

# Reset password
kubectl patch secret postgres-secret -n punjab-analysis -p '{"data":{"password":"'$(echo -n "newpassword" | base64)'"}}'
```

### 2. Storage Errors

#### "No space left on device"

```bash
# Check PVC usage
kubectl get pvc -n punjab-analysis

# Check disk usage
kubectl exec -it $(kubectl get pods -l component=webserver -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- df -h

# Clean up old data
kubectl run -it --rm cleanup-pod --image=busybox --restart=Never -n punjab-analysis -- sh -c "
find /data -name '*.csv' -mtime +7 -delete
find /output -name '*.csv' -mtime +7 -delete
"

# Resize PVC if needed
kubectl patch pvc punjab-data-pvc -n punjab-analysis -p '{"spec":{"resources":{"requests":{"storage":"20Gi"}}}}'
```

#### "Mount failed"

```bash
# Check PVC status
kubectl get pvc -n punjab-analysis

# Check PV status
kubectl get pv

# Check pod mounts
kubectl describe pod <pod-name> -n punjab-analysis | grep -A 10 "Mounts:"

# Recreate PVC if needed
kubectl delete pvc <pvc-name> -n punjab-analysis
kubectl apply -f k8s/06-persistent-volume-claims.yaml
```

### 3. Network Errors

#### "Connection timeout"

```bash
# Check service endpoints
kubectl get endpoints -n punjab-analysis

# Check network policies
kubectl get networkpolicies -n punjab-analysis

# Check DNS resolution
kubectl exec -it <pod-name> -n punjab-analysis -- nslookup postgres-external.punjab-analysis.svc.cluster.local

# Restart services
kubectl rollout restart deployment --all -n punjab-analysis
```

#### "Service unavailable"

```bash
# Check service status
kubectl get svc -n punjab-analysis

# Check pod readiness
kubectl get pods -n punjab-analysis

# Check service selector
kubectl describe svc <service-name> -n punjab-analysis
```

---

## ⚡ Performance Issues

### 1. Slow Task Execution

#### Diagnosis

```bash
# Check resource usage
kubectl top pods -n punjab-analysis

# Check node resources
kubectl top nodes

# Check task execution times
kubectl logs -l component=scheduler -n punjab-analysis | grep -i "duration"
```

#### Solutions

##### Increase Resources

```bash
# Increase pod resources
kubectl patch deployment airflow-scheduler -n punjab-analysis -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "scheduler",
          "resources": {
            "requests": {"cpu": "1", "memory": "2Gi"},
            "limits": {"cpu": "2", "memory": "4Gi"}
          }
        }]
      }
    }
  }
}'
```

##### Optimize Configuration

```bash
# Check chunk size configuration
kubectl exec -it $(kubectl get pods -l component=scheduler -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- cat /app/secrets/db_config.yaml | grep chunk_size

# Reduce chunk size for better performance
kubectl run -it --rm config-updater --image=busybox --restart=Never -n punjab-analysis -- sh -c "
sed -i 's/chunk_size: 50000/chunk_size: 25000/' /app/secrets/db_config.yaml
"
```

### 2. High Memory Usage

#### Diagnosis

```bash
# Check memory usage
kubectl top pods -n punjab-analysis --sort-by=memory

# Check memory limits
kubectl describe pods -n punjab-analysis | grep -A 5 "Limits:"

# Check for memory leaks
kubectl logs -l component=scheduler -n punjab-analysis | grep -i "memory"
```

#### Solutions

##### Increase Memory Limits

```bash
# Update memory limits
kubectl patch deployment airflow-scheduler -n punjab-analysis -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "scheduler",
          "resources": {
            "limits": {"memory": "8Gi"}
          }
        }]
      }
    }
  }
}'
```

##### Optimize Memory Usage

```bash
# Check for large files
kubectl exec -it $(kubectl get pods -l component=webserver -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- find /opt/airflow/logs -name "*.log" -size +100M

# Clean up large log files
kubectl exec -it $(kubectl get pods -l component=webserver -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- find /opt/airflow/logs -name "*.log" -size +100M -delete
```

---

## 🌐 Network Issues

### 1. Service Discovery Issues

#### Symptoms

- Pods cannot communicate with each other
- DNS resolution failures

#### Diagnosis

```bash
# Check DNS resolution
kubectl exec -it <pod-name> -n punjab-analysis -- nslookup kubernetes.default.svc.cluster.local

# Check service endpoints
kubectl get endpoints -n punjab-analysis

# Check network policies
kubectl get networkpolicies -n punjab-analysis
```

#### Solutions

##### DNS Issues

```bash
# Restart DNS pods
kubectl rollout restart deployment/coredns -n kube-system

# Check DNS configuration
kubectl get configmap coredns -n kube-system -o yaml
```

##### Service Issues

```bash
# Check service selector
kubectl describe svc <service-name> -n punjab-analysis

# Verify pod labels
kubectl get pods -n punjab-analysis --show-labels
```

### 2. External Connectivity Issues

#### Symptoms

- Cannot access Airflow UI from outside
- NodePort not working

#### Diagnosis

```bash
# Check NodePort
kubectl get svc airflow-webserver -n punjab-analysis

# Check Minikube IP
minikube ip

# Test connectivity
curl -I http://$(minikube ip):30082
```

#### Solutions

##### NodePort Issues

```bash
# Recreate service
kubectl delete svc airflow-webserver -n punjab-analysis
kubectl apply -f k8s/08-airflow-service.yaml

# Check firewall
sudo ufw status
```

##### Minikube Issues

```bash
# Restart Minikube
minikube stop
minikube start

# Check Minikube status
minikube status
```

---

## 💾 Storage Issues

### 1. PVC Issues

#### Symptoms

- Pods cannot mount volumes
- "Mount failed" errors

#### Diagnosis

```bash
# Check PVC status
kubectl get pvc -n punjab-analysis

# Check PV status
kubectl get pv

# Check storage class
kubectl get storageclass
```

#### Solutions

##### PVC Not Bound

```bash
# Check PV availability
kubectl get pv

# Check storage class
kubectl describe storageclass standard

# Recreate PVC
kubectl delete pvc <pvc-name> -n punjab-analysis
kubectl apply -f k8s/06-persistent-volume-claims.yaml
```

##### Permission Issues

```bash
# Check pod security context
kubectl describe pod <pod-name> -n punjab-analysis | grep -A 10 "Security Context:"

# Fix permissions
kubectl run -it --rm fix-permissions --image=busybox --restart=Never -n punjab-analysis -- sh -c "
chown -R 1000:1000 /data /output
chmod -R 755 /data /output
"
```

### 2. Disk Space Issues

#### Symptoms

- "No space left on device" errors
- Pods failing to start

#### Diagnosis

```bash
# Check disk usage
kubectl exec -it $(kubectl get pods -l component=webserver -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- df -h

# Check PVC usage
kubectl get pvc -n punjab-analysis
```

#### Solutions

##### Clean Up Data

```bash
# Clean up old data
kubectl run -it --rm cleanup-pod --image=busybox --restart=Never -n punjab-analysis -- sh -c "
find /data -name '*.csv' -mtime +7 -delete
find /output -name '*.csv' -mtime +7 -delete
find /opt/airflow/logs -name '*.log' -mtime +30 -delete
"
```

##### Resize PVC

```bash
# Resize PVC
kubectl patch pvc punjab-data-pvc -n punjab-analysis -p '{"spec":{"resources":{"requests":{"storage":"20Gi"}}}}'
kubectl patch pvc punjab-output-pvc -n punjab-analysis -p '{"spec":{"resources":{"requests":{"storage":"10Gi"}}}}'
```

---

## 🔄 Recovery Procedures

### 1. Complete System Recovery

#### Step 1: Stop All Services

```bash
kubectl scale deployment --all --replicas=0 -n punjab-analysis
```

#### Step 2: Clean Up Resources

```bash
kubectl delete pods --all -n punjab-analysis
kubectl delete services --all -n punjab-analysis
kubectl delete deployments --all -n punjab-analysis
```

#### Step 3: Restore from Backup

```bash
# Restore configurations
kubectl apply -f backup/dag-configmap.yaml
kubectl apply -f backup/secrets.yaml

# Restore database
kubectl apply -f k8s/postgres-deployment.yaml
kubectl wait --for=condition=ready pod -l app=postgres -n punjab-analysis --timeout=300s
kubectl exec -i $(kubectl get pods -l app=postgres -n punjab-analysis -o jsonpath='{.items[0].metadata.name}') -n punjab-analysis -- psql -U postgres -d airflowdb < backup/database_backup.sql
```

#### Step 4: Restart Services

```bash
kubectl apply -f k8s/07-airflow-deployment.yaml
kubectl apply -f k8s/08-airflow-service.yaml
```

### 2. Data Recovery

#### Recover from PVC Backup

```bash
# Create recovery pod
kubectl run -it --rm recovery-pod --image=busybox --restart=Never -n punjab-analysis -- sh -c "
tar -xzf /tmp/backup.tar.gz -C /
"

# Copy backup to pod
kubectl cp backup/data-backup.tar.gz punjab-analysis/recovery-pod:/tmp/backup.tar.gz
```

### 3. Configuration Recovery

#### Restore DAG Configuration

```bash
kubectl apply -f backup/dag-configmap.yaml
kubectl rollout restart deployment/airflow-scheduler -n punjab-analysis
```

#### Restore Database Configuration

```bash
kubectl apply -f backup/secrets.yaml
kubectl rollout restart deployment/postgres -n punjab-analysis
```

---

## 📞 Support & Escalation

### Level 1 Support (Basic Troubleshooting)

- Check pod status and logs
- Restart services
- Verify configurations

### Level 2 Support (Advanced Troubleshooting)

- Database recovery
- Storage issues
- Network problems

### Level 3 Support (System Recovery)

- Complete system recovery
- Data restoration
- Configuration restoration

### Emergency Contacts

- **System Administrator**: [Your Contact]
- **Database Administrator**: [DB Admin Contact]
- **Kubernetes Administrator**: [K8s Admin Contact]

### Documentation References

- **Technical Documentation**: `TECHNICAL_DOCUMENTATION.md`
- **Operational Runbooks**: `OPERATIONAL_RUNBOOKS.md`
- **User Training**: `USER_TRAINING_MATERIALS.md`

---

_This troubleshooting guide provides comprehensive solutions for common issues. Always test solutions in a non-production environment first and maintain backups before making changes._
