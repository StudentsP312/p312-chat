# Kubernetes (K8s) Deployment Guide

This directory contains the complete set of Kubernetes manifests for deploying the Chat Application in both local development environments (Minikube, Kind, k3s, MicroK8s) and production-grade managed cloud clusters (AWS EKS, Google Cloud GKE, Azure AKS, Yandex Managed Service for K8s).

---

## 🏗 Directory Structure & Manifests

| Manifest | Kind / Resource | Description |
|---|---|---|
| [`namespace.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/namespace.yaml) | `Namespace` | Defines the isolated `chat-app` namespace. |
| [`configmap.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/configmap.yaml) | `ConfigMap` | Non-sensitive configurations (DB pool, Redis URL, rate limits, spam filters, S3 bucket settings). |
| [`secret.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/secret.yaml) | `Secret` | Sensitive credentials (PostgreSQL credentials, `JWT_SECRET`, AWS S3 access keys). |
| [`postgres-statefulset.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/postgres-statefulset.yaml) | `StatefulSet`, `Service` | PostgreSQL 16 with a 10Gi PersistentVolumeClaim and ClusterIP service (dev / self-hosted). |
| [`redis-deployment.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/redis-deployment.yaml) | `Deployment`, `Service` | Redis 7 with Append-Only File (AOF) persistence and ClusterIP service. |
| [`migration-job.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/migration-job.yaml) | `Job` | One-shot Alembic migration job (`alembic upgrade head`) with a DB readiness init container. |
| [`api-deployment.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/api-deployment.yaml) | `Deployment`, `Service`, `HPA` | FastAPI service with RollingUpdate, Liveness/Readiness probes, graceful shutdown hook, and HPA. |
| [`celery-worker-deployment.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/celery-worker-deployment.yaml) | `Deployment` (x2) | Asynchronous Celery background workers and Celery Beat periodic task scheduler. |
| [`ingress.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/ingress.yaml) | `Ingress` | NGINX Ingress rules with WebSocket proxy timeout support, 25MB body upload limit, and TLS cert-manager. |
| [`kustomization.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/kustomization.yaml) | `Kustomization` | Kustomize orchestration manifest for applying and managing all resources together. |

---

## 📋 Prerequisites

Before deploying, ensure you have:
1. **`kubectl`** CLI installed and connected to your target cluster (`kubectl cluster-info`).
2. **Container Runtime / Engine** (e.g., Docker) to build container images.
3. **Ingress Controller** (e.g., `ingress-nginx`) installed in the cluster if exposing traffic externally.
4. **Metrics Server** installed in the cluster (required for Horizontal Pod Autoscaling / HPA).
5. **`cert-manager`** (optional) if using automated Let's Encrypt SSL/TLS certificates.

---

## 🚀 Quick Start: Local Cluster (Minikube / Kind)

Follow these steps to deploy the entire stack (including in-cluster PostgreSQL and Redis):

### 1. Build and Load Docker Image

Build the application image:
```bash
docker build -t chat-app:latest .
```

Load the image into your local cluster:
- **Minikube:**
  ```bash
  minikube image load chat-app:latest
  ```
- **Kind:**
  ```bash
  kind load docker-image chat-app:latest
  ```

### 2. Apply Manifests via Kustomize

Deploy all components in one step:
```bash
kubectl apply -k k8s/
```

### 3. Verify Deployment Status

Check the status of all pods, services, and workloads:
```bash
# Watch pods transitioning to Running
kubectl get pods -n chat-app -w

# Check services
kubectl get svc -n chat-app

# Check Ingress and HPA
kubectl get ingress -n chat-app
kubectl get hpa -n chat-app
```

### 4. Access the Application Locally

- **Via Port-Forwarding (Fastest for testing):**
  ```bash
  kubectl port-forward svc/chat-api-service 8000:8000 -n chat-app
  ```
  The API will be accessible at `http://localhost:8000` (docs at `http://localhost:8000/docs`).

- **Via Minikube Ingress / Tunnel:**
  ```bash
  minikube tunnel
  ```
  Add `127.0.0.1 chat.example.com` to your `/etc/hosts` (or `C:\Windows\System32\drivers\etc\hosts`) and navigate to `http://chat.example.com`.

---

## ☁️ Production Deployment (Cloud Managed DB, Redis & S3)

In production environments, managed services (e.g., AWS RDS/Aurora, ElastiCache, GCP Cloud SQL, S3/GCS) are recommended over in-cluster databases.

### 1. Exclude In-Cluster Postgres & Redis

Remove or comment out in-cluster database manifests in [`k8s/kustomization.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/kustomization.yaml):

```yaml
resources:
  - namespace.yaml
  - configmap.yaml
  - secret.yaml
  # - postgres-statefulset.yaml   # Comment out for managed DB
  # - redis-deployment.yaml        # Comment out for managed Redis
  - migration-job.yaml
  - api-deployment.yaml
  - celery-worker-deployment.yaml
  - ingress.yaml
```

### 2. Update Configuration in `configmap.yaml`

Set external endpoints, bucket names, and regions in [`k8s/configmap.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/configmap.yaml):

```yaml
POSTGRES_SERVER: "rds-postgres-prod.xxxxxx.us-east-1.rds.amazonaws.com"
POSTGRES_PORT: "5432"
POSTGRES_DB: "chat_prod_db"
REDIS_URL: "redis://elasticache-redis.xxxxxx.0001.use1.cache.amazonaws.com:6379/0"
S3_BUCKET: "my-production-chat-bucket"
S3_REGION_NAME: "us-east-1"
S3_ENDPOINT_URL: "https://s3.us-east-1.amazonaws.com"
```

### 3. Update Secrets in `secret.yaml`

Update credentials in [`k8s/secret.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/secret.yaml) (or use SealedSecrets / External Secrets Operator):

```yaml
stringData:
  POSTGRES_USER: "chat_prod_user"
  POSTGRES_PASSWORD: "strong_production_password"
  JWT_SECRET: "replace_with_64_char_secure_random_hex_string"
  AWS_ACCESS_KEY_ID: "AKIAXXXXXXXXXXXXXXXX"
  AWS_SECRET_ACCESS_KEY: "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
```

### 4. Configure Ingress Host & SSL in `ingress.yaml`

Update the domain host and TLS certificate configuration in [`k8s/ingress.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/ingress.yaml):

```yaml
spec:
  tls:
    - hosts:
        - chat.yourdomain.com
      secretName: chat-production-tls
  rules:
    - host: chat.yourdomain.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: chat-api-service
                port:
                  number: 8000
```

### 5. Deploy to Production

```bash
kubectl apply -k k8s/
```

---

## 🔄 Database Migrations (Alembic)

Database migrations run automatically during deployment via the Kubernetes Job in [`k8s/migration-job.yaml`](file:///D:/netfolder/P311/cloud/p312-chat/k8s/migration-job.yaml).

- An **init container** (`wait-for-postgres`) polls `pg_isready` until the PostgreSQL server is reachable.
- The **main container** executes `alembic upgrade head`.

### Manual Migration Execution

To trigger migrations manually:
```bash
# Delete any existing completed or failed migration job
kubectl delete job chat-migration -n chat-app --ignore-not-found

# Run the migration job
kubectl apply -f k8s/migration-job.yaml

# Stream migration logs
kubectl logs -n chat-app job/chat-migration -f
```

---

## ⚙️ Workload Details & Scaling

### FastAPI Application (`chat-api`)
- **Probes:** Configured with `/health` liveness probe (15s initial delay, 10s period) and readiness probe (5s initial delay, 5s period).
- **Graceful Shutdown:** `preStop` hook pauses 5 seconds to drain active in-flight requests during rolling updates.
- **Autoscaling (HPA):** Scales between **2** and **10** replicas based on target metrics (70% CPU utilization, 80% Memory utilization).

### Celery Workers & Beat
- **`chat-celery-worker`:** Processes async tasks (thumbnails, cleanup, email notifications). Scales horizontally via Deployment replica count.
- **`chat-celery-beat`:** Single-replica (`Recreate` strategy) scheduler for periodic tasks. Employs `celery-redbeat` distributed Redis locks for high availability.

### Ingress & WebSockets
- Includes NGINX annotations:
  - `nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"` and `proxy-send-timeout: "3600"` to maintain long-lived WebSocket connections.
  - `nginx.ingress.kubernetes.io/proxy-body-size: "25m"` to support media uploads up to 25MB.
  - `nginx.ingress.kubernetes.io/websocket-services: "chat-api-service"` for upstream WebSocket upgrades.

---

## 🛠 Useful Troubleshooting Commands

```bash
# View all resources in chat-app namespace
kubectl get all -n chat-app

# Inspect API logs
kubectl logs -n chat-app -l app=chat-api -f --tail=100

# Inspect Celery worker logs
kubectl logs -n chat-app -l app=chat-celery-worker -f --tail=100

# Check HPA scaling status
kubectl describe hpa chat-api-hpa -n chat-app

# Troubleshoot pod restart or failure
kubectl describe pod <pod-name> -n chat-app
```

---

## 🧹 Teardown

To delete all resources in the `chat-app` namespace:
```bash
kubectl delete -k k8s/
```
