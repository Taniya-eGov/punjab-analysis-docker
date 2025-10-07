#!/bin/bash

# Punjab Analysis System - Kubernetes Deployment Script
# Deploys the complete system to Kubernetes

set -e  # Exit on any error

echo "🚀 Punjab Analysis System - Kubernetes Deployment"
echo "=================================================="

# Configuration
NAMESPACE="punjab-analysis"
IMAGE_NAME="punjab-analysis"
IMAGE_TAG="v3.0.0"
FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "❌ kubectl is not installed or not in PATH"
    exit 1
fi

# Check if minikube is running
echo "🔍 Checking Kubernetes cluster status..."
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ Kubernetes cluster is not accessible"
    echo "💡 Try: minikube start"
    exit 1
fi

echo "✅ Kubernetes cluster is accessible"

# Load Docker image into minikube
echo "📦 Loading Docker image into minikube..."
if command -v minikube &> /dev/null; then
    echo "Loading ${FULL_IMAGE} into minikube..."
    minikube image load ${FULL_IMAGE} || echo "⚠️  Image may already be loaded"
else
    echo "⚠️  Minikube not found, assuming image is available in cluster"
fi

# Create namespace
echo "🏗️  Creating namespace: ${NAMESPACE}"
kubectl apply -f 01-namespace.yaml

# Apply all manifests in order
echo "📋 Applying ConfigMap..."
kubectl apply -f 02-configmap.yaml

echo "🔐 Applying Secrets..."
kubectl apply -f 03-secrets.yaml

echo "🔗 Applying External PostgreSQL Service..."
kubectl apply -f 04-postgres-external-service.yaml

echo "💾 Applying Persistent Volumes..."
kubectl apply -f 05-persistent-volumes.yaml

echo "📁 Applying Persistent Volume Claims..."
kubectl apply -f 06-persistent-volume-claims.yaml

echo "🔒 Applying RBAC configuration..."
kubectl apply -f 09-rbac.yaml

echo "📄 Applying DAG ConfigMap..."
kubectl apply -f 10-dag-configmap.yaml

echo "🚁 Applying Airflow Deployment..."
kubectl apply -f 07-airflow-deployment.yaml

echo "🌐 Applying Airflow Service..."
kubectl apply -f 08-airflow-service.yaml

# Wait for deployments to be ready
echo "⏳ Waiting for Airflow deployments to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/airflow-webserver -n ${NAMESPACE}
kubectl wait --for=condition=available --timeout=300s deployment/airflow-scheduler -n ${NAMESPACE}

# Get service information
echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "📊 Cluster Status:"
kubectl get pods -n ${NAMESPACE}
echo ""
echo "🌐 Services:"
kubectl get svc -n ${NAMESPACE}
echo ""

# Get Airflow access information
MINIKUBE_IP=$(minikube ip 2>/dev/null || echo "localhost")
NODE_PORT=$(kubectl get svc airflow-webserver -n ${NAMESPACE} -o jsonpath='{.spec.ports[0].nodePort}')

echo "🎯 Access Information:"
echo "   Airflow UI: http://${MINIKUBE_IP}:${NODE_PORT}"
echo "   Username: admin"
echo "   Password: admin123"
echo ""

# Copy the Kubernetes DAG to the mounted directory
echo "📄 Copying Kubernetes DAG to Airflow..."
DAG_SOURCE="punjab_analysis_kubernetes_dag.py"
DAG_TARGET="../airflow/dags/punjab_analysis_kubernetes_dag.py"

if [ -f "${DAG_SOURCE}" ]; then
    cp "${DAG_SOURCE}" "${DAG_TARGET}"
    echo "✅ Kubernetes DAG copied to: ${DAG_TARGET}"
else
    echo "⚠️  Kubernetes DAG not found: ${DAG_SOURCE}"
fi

echo ""
echo "🎉 Punjab Analysis System is now running on Kubernetes!"
echo ""
echo "📝 Next steps:"
echo "   1. Access Airflow UI: http://${MINIKUBE_IP}:${NODE_PORT}"
echo "   2. Enable the 'punjab_analysis_kubernetes_pipeline' DAG"
echo "   3. Trigger a manual run to test the system"
echo "   4. Monitor pod creation and execution"
echo ""
echo "🔍 Useful commands:"
echo "   kubectl get pods -n ${NAMESPACE} -w"
echo "   kubectl logs -f deployment/airflow-scheduler -n ${NAMESPACE}"
echo "   kubectl describe pod <pod-name> -n ${NAMESPACE}"