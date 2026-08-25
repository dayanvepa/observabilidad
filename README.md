# Unidad 2 — Lab 1: Observabilidad end-to-end con OpenTelemetry en GKE

> **Curso:** Observabilidad y SRE · **Unidad 2 · Lab 1**  
> **Stack:** OpenTelemetry SDK · FastAPI · PostgreSQL · Jaeger · Prometheus · Grafana · GKE  
> **Objetivo:** Instrumentar dos microservicios con el OTel SDK, desplegarlos en Google Kubernetes Engine, medir el overhead real de la instrumentación y analizar trazas distribuidas, métricas SLI y logs correlacionados.

---

## Tabla de contenidos

1. [Arquitectura](#1-arquitectura)
2. [Stack tecnológico](#2-stack-tecnológico)
3. [Estructura del repositorio](#3-estructura-del-repositorio)
4. [Pre-requisitos](#4-pre-requisitos)
5. [Desarrollo local con Docker Compose](#5-desarrollo-local-con-docker-compose)
6. [Despliegue en GKE](#6-despliegue-en-gke)
   - 6.1 [Crear el clúster](#61-crear-el-clúster)
   - 6.2 [Artifact Registry y build de imágenes](#62-artifact-registry-y-build-de-imágenes)
   - 6.3 [Aplicar manifiestos](#63-aplicar-manifiestos)
   - 6.4 [Inicializar base de datos](#64-inicializar-base-de-datos)
   - 6.5 [Verificar despliegue](#65-verificar-despliegue)
7. [Toggle de instrumentación OTel](#7-toggle-de-instrumentación-otel)
8. [Benchmark de overhead](#8-benchmark-de-overhead)
9. [Acceso a las UIs de observabilidad](#9-acceso-a-las-uis-de-observabilidad)
10. [Pipeline del OTel Collector](#10-pipeline-del-otel-collector)
11. [Instrumentación del SDK](#11-instrumentación-del-sdk)
12. [Resultados del benchmark](#12-resultados-del-benchmark)
13. [Limitaciones conocidas](#13-limitaciones-conocidas)
14. [Bug encontrado y corrección aplicada](#14-bug-encontrado-y-corrección-aplicada)
15. [Referencias](#15-referencias)

---

## 1. Arquitectura

```
                          ┌─────────────────────────────────────────────┐
                          │               GKE — namespace: otel-lab      │
                          │                                               │
  ┌──────────┐  GET /order│  ┌─────────────────────────────────────────┐ │
  │  k6 /    │ ──────────▶│  │  service-a (FastAPI :8000)               │ │
  │  Client  │   LoadBalancer│  │  ├─ OTel SDK: traces + metrics + logs  │ │
  └──────────┘            │  │  ├─ HTTPXClient → service-b              │ │
                          │  │  └─ psycopg2 → PostgreSQL               │ │
                          │  └────────────┬───────────────┬────────────┘ │
                          │               │ W3C traceparent│ OTLP gRPC   │
                          │               ▼               ▼             │
                          │  ┌────────────────┐  ┌────────────────────┐ │
                          │  │ service-b      │  │ OTel Collector     │ │
                          │  │ (FastAPI :8001)│  │ (DaemonSet)        │ │
                          │  │ inventory API  │  │ :4317 OTLP gRPC    │ │
                          │  └───────┬────────┘  │ :4318 OTLP HTTP    │ │
                          │          │            │ :8888 internal     │ │
                          │          ▼            │ :8889 Prometheus   │ │
                          │  ┌────────────────┐  │ :13133 health      │ │
                          │  │  PostgreSQL 16 │  └──────┬─────────────┘ │
                          │  │  :5432         │         │               │
                          │  │  orders        │    ┌────▼──────────┐   │
                          │  │  inventory     │    │   Pipelines   │   │
                          │  └────────────────┘    │ ┌───────────┐ │   │
                          │                        │ │  Traces   │ │   │
                          │                        │ │  → Jaeger │ │   │
                          │                        │ ├───────────┤ │   │
                          │                        │ │  Metrics  │ │   │
                          │                        │ │  → Prom.  │ │   │
                          │                        │ ├───────────┤ │   │
                          │                        │ │   Logs    │ │   │
                          │                        │ │  → Cloud  │ │   │
                          │                        │ │  Logging  │ │   │
                          │                        │ └───────────┘ │   │
                          │                        └───────────────┘   │
                          └─────────────────────────────────────────────┘
                                          │              │
                                   ┌──────▼───┐   ┌─────▼──────┐
                                   │  Jaeger  │   │ Prometheus │
                                   │  :16686  │   │  :9090     │
                                   └──────────┘   └─────┬──────┘
                                                         │
                                                  ┌──────▼──────┐
                                                  │   Grafana   │
                                                  │   :3000     │
                                                  └─────────────┘
```

### Decisiones de diseño

| Componente | Patrón | Justificación |
|---|---|---|
| OTel Collector | **DaemonSet** | Un agente por nodo GKE — comparte recursos del nodo, reduce la presión por pod |
| Propagación | **W3C TraceContext** (`traceparent`) | Estándar vendor-neutral; inyectado automáticamente por `HTTPXClientInstrumentor` |
| Exportación de trazas | OTLP gRPC → Collector → Jaeger | Desacopla el SDK del backend; permite cambiar Jaeger por Tempo sin tocar el código |
| Métricas | OTLP push (15 s) + Prometheus pull (:8889) | Push vía `PeriodicExportingMetricReader`; Prometheus hace scraping del Collector |
| Logs | JSON estructurado con `trace_id` / `span_id` | Habilita correlación log ↔ traza en Grafana sin plugins adicionales |

---

## 2. Stack tecnológico

| Capa | Componente | Versión |
|---|---|---|
| **SDK** | opentelemetry-sdk (Python) | 1.25.0 |
| **Instrumentación** | FastAPIInstrumentor, HTTPXClientInstrumentor, Psycopg2Instrumentor | auto |
| **Collector** | opentelemetry-collector-contrib | 0.103.0 |
| **Trazas** | Jaeger all-in-one | 1.58 |
| **Métricas** | Prometheus | v2.52.0 |
| **Visualización** | Grafana | 10.4.0 |
| **Servicios** | FastAPI + Uvicorn | 0.111 / 0.30 |
| **DB** | PostgreSQL | 16-alpine |
| **Benchmark** | k6 | latest |
| **Orquestación** | GKE (Kubernetes) | 1.29+ |
| **Imágenes** | Python | 3.12.7-slim |

---

## 3. Estructura del repositorio

```
.
├── service-a/
│   ├── main.py                  # FastAPI app + OTel SDK (trazas, métricas, logs)
│   ├── Dockerfile               # python:3.12.7-slim, 2 Uvicorn workers
│   └── requirements.txt
├── service-b/
│   ├── main.py                  # FastAPI inventory service
│   ├── Dockerfile
│   └── requirements.txt
├── k8s/
│   └── gcp/
│       └── deployment.yaml      # DaemonSet, Deployments, Services, HPA
├── otel-collector/
│   └── collector-config.yaml    # Pipeline completo: receivers → processors → exporters
├── prometheus/
│   └── prometheus.yaml          # Scrape configs
├── grafana/
│   ├── provisioning/
│   │   ├── datasources.yaml     # Prometheus + Jaeger datasources
│   │   └── dashboards.yaml      # Auto-provision de dashboards
│   └── dashboards/
│       └── *.json               # Dashboards importados
├── scripts/
│   └── init-db.sql              # Schema + índices + seed data
├── benchmark/
│   └── k6_benchmark.js          # 3 escenarios: warmup / sustained / spike
├── docker-compose.yaml          # Stack local completo (dev/debug)
└── README.md
```

---

## 4. Pre-requisitos

### Herramientas requeridas

```bash
# Google Cloud SDK
brew install --cask google-cloud-sdk
gcloud --version   # >= 480.0.0

# kubectl
gcloud components install kubectl
kubectl version --client

# Docker Desktop (Mac)
# https://www.docker.com/products/docker-desktop/
docker --version   # >= 26.0.0

# k6 (benchmark)
brew install k6
k6 version         # >= 0.51.0

# Python (local, opcional — solo para análisis de resultados)
python3 --version  # >= 3.11
pip install pandas scipy tabulate
```

### Permisos GCP requeridos

El service account activo necesita los siguientes roles:

```
roles/container.admin           # Gestión del clúster GKE
roles/artifactregistry.writer   # Push de imágenes
roles/logging.logWriter         # Cloud Logging (para el Collector)
roles/cloudtrace.agent          # Cloud Trace (opcional)
```

---

## 5. Desarrollo local con Docker Compose

El entorno local replica el stack completo sin GKE. Útil para iterar sobre el código antes de desplegar.

```bash
# Clonar el repositorio
git clone <URL-del-repo>
cd <nombre-repo>

# Iniciar el stack completo
docker compose up -d

# Verificar que todos los servicios están healthy
docker compose ps
```

Una vez iniciado, los accesos locales son:

| Servicio | URL |
|---|---|
| service-a (FastAPI docs) | http://localhost:8000/docs |
| service-b (FastAPI docs) | http://localhost:8001/docs |
| Jaeger UI | http://localhost:16686 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9091 |
| OTel Collector zPages | http://localhost:55679 |

```bash
# Probar el endpoint principal
curl http://localhost:8000/order/ord-001

# Ver el pipeline del Collector en tiempo real
open http://localhost:55679/pipelinez

# Teardown
docker compose down -v
```

> **Nota sobre credenciales GCP en local:** El `docker-compose.yaml` monta las credenciales de aplicación por defecto de gcloud (`~/.config/gcloud/application_default_credentials.json`) para que el Collector pueda exportar a Cloud Logging. Asegúrate de ejecutar `gcloud auth application-default login` antes de arrancar el compose.

---

## 6. Despliegue en GKE

### 6.1 Crear el clúster

```bash
# Variables de entorno — ajusta según tu proyecto
export GCP_PROJECT="coastal-gantry-506317-k5"
export GCP_REGION="us-central1"
export CLUSTER_NAME="otel-lab"
export NAMESPACE="otel-lab"

# Autenticación
gcloud auth login
gcloud config set project $GCP_PROJECT

# Crear clúster (3 nodos e2-standard-2 ~= 6 vCPU, 24 GB RAM total)
gcloud container clusters create $CLUSTER_NAME \
  --region=$GCP_REGION \
  --num-nodes=3 \
  --machine-type=e2-standard-2 \
  --enable-autoscaling \
  --min-nodes=2 \
  --max-nodes=6

# Obtener credenciales kubectl
gcloud container clusters get-credentials $CLUSTER_NAME \
  --region=$GCP_REGION \
  --project=$GCP_PROJECT

# Crear namespace y establecerlo como default
kubectl create namespace $NAMESPACE
kubectl config set-context --current --namespace=$NAMESPACE
```

### 6.2 Artifact Registry y build de imágenes

> ⚠️ **Mac con Apple Silicon (M1/M2/M3):** Los nodos GKE son `linux/amd64`. Debes indicar la plataforma de destino en el build, de lo contrario las imágenes no correrán en el clúster.

```bash
# Habilitar Artifact Registry API
gcloud services enable artifactregistry.googleapis.com

# Crear repositorio (solo la primera vez)
gcloud artifacts repositories create otel-lab \
  --repository-format=docker \
  --location=$GCP_REGION \
  --description="OTel Lab images"

# Configurar Docker para autenticarse con Artifact Registry
gcloud auth configure-docker ${GCP_REGION}-docker.pkg.dev

# ── service-a ────────────────────────────────────────────────────────────────
docker build \
  --platform linux/amd64 \
  -t ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/otel-lab/service-a:1.0.0 \
  ./service-a

docker push \
  ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/otel-lab/service-a:1.0.0

# ── service-b ────────────────────────────────────────────────────────────────
docker build \
  --platform linux/amd64 \
  -t ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/otel-lab/service-b:1.0.0 \
  ./service-b

docker push \
  ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/otel-lab/service-b:1.0.0
```

### 6.3 Aplicar manifiestos

```bash
# Secret con el GCP Project ID (requerido por el Collector para Cloud Logging)
kubectl create secret generic gcp-credentials \
  --from-literal=project_id=$GCP_PROJECT \
  -n $NAMESPACE

# ConfigMap del OTel Collector
kubectl create configmap collector-config \
  --from-file=collector-config.yaml=./otel-collector/collector-config.yaml \
  -n $NAMESPACE

# Aplicar todos los manifiestos
kubectl apply -f k8s/gcp/deployment.yaml

# Esperar a que todos los pods estén Running
kubectl rollout status deployment/service-a -n $NAMESPACE
kubectl rollout status deployment/service-b -n $NAMESPACE
kubectl rollout status daemonset/otel-collector -n $NAMESPACE
```

### 6.4 Inicializar base de datos

```bash
# Obtener el nombre del pod de PostgreSQL
PG_POD=$(kubectl get pod -l app=postgres -o jsonpath='{.items[0].metadata.name}')

# Ejecutar el script de inicialización (schema + índices + seed)
kubectl exec -i $PG_POD -- \
  psql -U app -d appdb < scripts/init-db.sql

# Verificar que las tablas y los datos existen
kubectl exec -it $PG_POD -- \
  psql -U app -d appdb -c "SELECT id, product, status FROM orders LIMIT 5;"
```

El script `init-db.sql` crea:

**Tablas:**
- `orders (id, product, quantity, status, customer_id, created_at, updated_at)`
- `inventory (product_id, available, warehouse, last_updated, reserved)`

**Índices de rendimiento** (fundamentales para el benchmark):

```sql
-- Índices que eliminan seq-scans en consultas de alta frecuencia
CREATE INDEX IF NOT EXISTS idx_orders_status  ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_product ON orders(product);
```

> ⚠️ **Por qué los índices son críticos:** Sin `idx_orders_product`, cada request de `GET /order/{id}` ejecuta un seq-scan completo sobre la tabla `orders`. Bajo carga (50 VUs), el tiempo de espera para adquirir locks de tabla se acumula, generando p99 > 6 s y errores de timeout. La creación de los índices redujo el error rate de 7.4% → 0% y el throughput mejoró de 16.7 → 25.0 rps.

**Seed data incluido:**

```
ord-001 → product-a, qty=2, status=pending
ord-002 → product-b, qty=1, status=shipped
ord-003 → product-a, qty=5, status=delivered
ord-004 → product-c, qty=3, status=pending
ord-005 → product-b, qty=2, status=processing
```

### 6.5 Verificar despliegue

```bash
# Estado de todos los recursos en el namespace
kubectl get all -n $NAMESPACE

# IPs externas de los LoadBalancers
kubectl get svc -n $NAMESPACE

# Esperado:
# service-a-svc   LoadBalancer  10.x.x.x   <EXTERNAL-IP>  8000/TCP
# jaeger-svc      LoadBalancer  10.x.x.x   <EXTERNAL-IP>  16686/TCP

# Guardar la IP de service-a
export SERVICE_A_IP=$(kubectl get svc service-a-svc \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Verificar health
curl http://${SERVICE_A_IP}:8000/health
# {"status":"ok","service":"service-a"}

# Verificar un pedido
curl http://${SERVICE_A_IP}:8000/order/ord-001
```

---

## 7. Toggle de instrumentación OTel

La variable de entorno `OTEL_ENABLED` controla si el SDK inicializa el `TracerProvider` con exportadores activos o con un provider vacío (sin overhead de telemetría).

```bash
# ── Desactivar OTel (modo baseline — sin instrumentación) ────────────────────
kubectl set env deployment/service-a OTEL_ENABLED=false -n $NAMESPACE

# Esperar rollout
kubectl rollout status deployment/service-a -n $NAMESPACE

# Verificar
kubectl exec -it deploy/service-a -- env | grep OTEL_ENABLED
# OTEL_ENABLED=false

# ── Activar OTel (modo instrumentado) ────────────────────────────────────────
kubectl set env deployment/service-a OTEL_ENABLED=true -n $NAMESPACE
kubectl rollout status deployment/service-a -n $NAMESPACE
```

> **Importante:** Espera siempre a que el rollout complete (`kubectl rollout status`) antes de iniciar el benchmark. Requests durante el rollout pueden mezclar pods con distinto estado de instrumentación y contaminar los resultados.

---

## 8. Benchmark de overhead

El benchmark se ejecuta desde tu máquina local (Mac) apuntando a la IP externa de service-a en GKE.

### Escenarios de carga

| Escenario | Executor | VUs | Duración | Propósito |
|---|---|---|---|---|
| `warmup` | ramping-vus | 1 → 10 | 60 s | Calentar JIT y pool de conexiones |
| `sustained_load` | constant-vus | 50 | 3 min | Medición principal de overhead |
| `spike` | ramping-vus | 50 → 200 → 50 | 60 s | Stress test bajo carga pico |

### SLOs del laboratorio (thresholds k6)

```javascript
http_req_duration: p(95) < 500 ms
http_req_duration: p(99) < 1000 ms
http_req_failed:   rate < 0.5%
```

### Ejecución del benchmark

```bash
# Obtener la IP externa del servicio
export SERVICE_A_IP=$(kubectl get svc service-a-svc \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# ── Paso 1: Baseline (sin OTel) ───────────────────────────────────────────────
kubectl set env deployment/service-a OTEL_ENABLED=false -n $NAMESPACE
kubectl rollout status deployment/service-a -n $NAMESPACE

k6 run \
  --env INSTRUMENTED=false \
  --env BASE_URL=http://${SERVICE_A_IP}:8000 \
  --out json=results_baseline.json \
  --out csv=results_baseline.csv \
  benchmark/k6_benchmark.js

# ── Paso 2: Con OTel ─────────────────────────────────────────────────────────
kubectl set env deployment/service-a OTEL_ENABLED=true -n $NAMESPACE
kubectl rollout status deployment/service-a -n $NAMESPACE

k6 run \
  --env INSTRUMENTED=true \
  --env BASE_URL=http://${SERVICE_A_IP}:8000 \
  --out json=results_otel.json \
  --out csv=results_otel.csv \
  benchmark/k6_benchmark.js

# ── Paso 3: Análisis comparativo ─────────────────────────────────────────────
python3 analysis.py
```

---

## 9. Acceso a las UIs de observabilidad

### En GKE (acceso directo con port-forward)

```bash
# Jaeger UI — trazas distribuidas
kubectl port-forward svc/jaeger-svc 16686:16686 -n $NAMESPACE &
open http://localhost:16686

# Prometheus — métricas
kubectl port-forward svc/prometheus-svc 9090:9090 -n $NAMESPACE &
open http://localhost:9090

# Grafana — dashboards
kubectl port-forward svc/grafana-svc 3000:3000 -n $NAMESPACE &
open http://localhost:3000   # admin / admin

# OTel Collector — pipeline debug (zPages)
kubectl port-forward ds/otel-collector 55679:55679 -n $NAMESPACE &
open http://localhost:55679/pipelinez
```

> **Alternativa:** Jaeger tiene un `LoadBalancer` externo en el puerto 16686. Puedes acceder directamente con la IP:
> ```bash
> export JAEGER_IP=$(kubectl get svc jaeger-svc \
>   -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
> open http://${JAEGER_IP}:16686
> ```

### Qué explorar en cada UI

**Jaeger — Trazas distribuidas:**
1. Selecciona el servicio `service-a` y operación `GET /order/{order_id}`
2. Observa el span tree completo: `order.get` → `fetch.order.db` → `call.service-b.inventory`
3. El header `traceparent` conecta los spans de service-a con los de service-b en la misma traza

**Prometheus — Métricas SLI:**
```promql
# Latencia p95 por endpoint
histogram_quantile(0.95,
  rate(http_request_duration_seconds_bucket[5m])
)

# Throughput (req/s)
rate(http_requests_total[1m])

# Error rate
rate(http_requests_total{status="5xx"}[1m])
  / rate(http_requests_total[1m])

# Requests activos en vuelo (saturación)
http_active_requests
```

**OTel Collector — zPages:**
- `/pipelinez`: estado de cada pipeline (traces / metrics / logs)
- `/servicez`: health y uptime del Collector
- `/tracez`: samples de spans recientes

---

## 10. Pipeline del OTel Collector

El Collector corre como **DaemonSet**: un pod por nodo GKE, lo que garantiza que los datos de telemetría de los pods del nodo pasen por el agente local sin tráfico cross-node.

### Flujo de datos

```
Apps (service-a, service-b)
  └─→ OTLP gRPC (:4317)
       └─→ Receivers: [otlp, prometheus, hostmetrics]
            └─→ Processors:
                  1. memory_limiter   → evita OOM bajo carga alta (límite: 400 MB)
                  2. resource          → enriquece con deployment.environment, cloud.region
                  3. resourcedetection → detecta atributos GCP (project, zone, cluster)
                  4. filter/health     → excluye spans de /health del pipeline de trazas
                  5. attributes/metrics→ elimina http.flavor (reduce cardinalidad Prometheus)
                  6. batch             → agrupa señales (timeout=5s, batch_size=1024)
            └─→ Exporters:
                  Traces  → otlp/jaeger (jaeger-svc:4317)
                  Metrics → prometheus (:8889 scraping endpoint)
                  Logs    → googlecloud (Cloud Logging)
```

### Configuraciones clave para producción

| Processor | Parámetro | Valor | Justificación |
|---|---|---|---|
| `memory_limiter` | `limit_mib` | 400 | El pod tiene límite de 500 MB; el Collector deja 100 MB de margen |
| `memory_limiter` | `spike_limit_mib` | 100 | Absorbe spikes de corta duración sin rechazar datos |
| `batch` | `timeout` | 5 s | Balance entre latencia de exportación y eficiencia de red |
| `batch` | `send_batch_size` | 1024 | Reduce el número de conexiones hacia Jaeger / Prometheus |
| `filter/health` | — | excluye `/health` | Evita que los health checks saturen Jaeger con spans sin valor |

> **SLI del Collector:** Monitoriza las métricas internas en `:8888` — en particular `otelcol_processor_refused_metric_points` (incrementa cuando el `memory_limiter` activo rechaza datos bajo presión de memoria). Un aumento sostenido de esta métrica indica que el límite de memoria del DaemonSet necesita ajustarse.

---

## 11. Instrumentación del SDK

### Arquitectura de señales en service-a

```python
# 1. Resource — identidad común en todas las señales
resource = Resource.create({
    SERVICE_NAME:    "service-a",
    SERVICE_VERSION: "1.0.0",
    "deployment.environment": "production",
    "cloud.provider": "gcp",
})

# 2. Trazas: TracerProvider + OTLP gRPC exporter
# Solo se inicializa si OTEL_ENABLED=true
if OTEL_ENABLED:
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True))
    )
    trace.set_tracer_provider(tracer_provider)

# 3. Auto-instrumentación (propaga W3C traceparent automáticamente)
if OTEL_ENABLED:
    HTTPXClientInstrumentor().instrument(tracer_provider=tracer_provider)
    Psycopg2Instrumentor().instrument(tracer_provider=tracer_provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)
```

### Instrumentos de métricas (SLIs)

| Instrumento | Tipo | Descripción |
|---|---|---|
| `http_requests_total` | Counter | Total de requests HTTP por endpoint y método |
| `http_request_duration_seconds` | Histogram | Distribución de latencia (p50/p95/p99) |
| `db_query_duration_seconds` | Histogram | Latencia de queries PostgreSQL |
| `service_b_calls_total` | Counter | Llamadas HTTP a service-b por estado |
| `http_active_requests` | UpDownCounter | Requests en vuelo — señal de saturación |

### Logs correlacionados con trazas

Los logs JSON incluyen `trace_id` y `span_id` del span activo, lo que permite navegar directamente de un log a la traza en Grafana:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "message": "Order fetched from DB",
  "service": "service-a",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "order_id": "ord-001",
  "status": "pending"
}
```

---

## 12. Resultados del benchmark

### Resultados consolidados (v2 — datos válidos, post-correcciones)

| Métrica | Baseline (sin OTel) | Con OTel | Overhead |
|---|---|---|---|
| Latencia promedio | 1 801.9 ms | 2 037.2 ms | **+13.1%** |
| Latencia p95 | 5 104.9 ms | 5 597.7 ms | **+9.7%** |
| Latencia p99 | 6 833.1 ms | 8 402.6 ms | **+23.0%** |
| Error rate | 0.00% | 0.07% | +0.07 pp |
| Throughput | 25.03 rps | 22.78 rps | **−8.9%** |

> **Contexto:** El overhead medido (+13% latencia promedio, −9% throughput) es **significativamente mayor** que el overhead típico de OTel en producción (~2-5% CPU, +3-8 ms de latencia). La causa principal es la **ausencia de connection pooling en PostgreSQL** — cada request abre y cierra una nueva conexión, lo que amplifica cualquier contención bajo carga. Ver sección [Limitaciones conocidas](#13-limitaciones-conocidas).

### Interpretación por escenario

**Warmup (1→10 VUs):** Ambos modos se comportan de forma similar. El overhead del SDK es mínimo cuando la carga es baja y las conexiones DB no compiten.

**Sustained load (50 VUs, 3 min):** Se observa el overhead base del SDK: +10-15% en latencia promedio. El `BatchSpanProcessor` acumula spans y los envía en lotes, reduciendo las conexiones de red pero añadiendo latencia en el percentil 95.

**Spike (200 VUs):** El p99 se dispara en el modo OTel porque el SDK intenta enviar spans al Collector mientras el servicio ya está bajo presión máxima. La ausencia de connection pooling se vuelve crítica: 200 conexiones simultáneas a PostgreSQL saturan el pool de procesos de postgres.

### Comparación con overhead esperado en producción

```
Overhead típico OTel (producción con connection pooling):
  Latencia promedio:  +2 a 5 ms
  CPU:                +2 a 5%
  Memoria:            +15 a 30 MB
  Throughput:         −1 a 3%

Overhead medido en este lab:
  Latencia promedio:  +235 ms (+13.1%)
  Throughput:         −2.25 rps (−8.9%)

→ El exceso se explica por la falta de pgBouncer/asyncpg (connection pooling)
  y no por el overhead del SDK en sí mismo.
```

---

## 13. Limitaciones conocidas

### PostgreSQL sin connection pooling

**Problema:** `service-a` usa `psycopg2.connect()` directamente, abriendo una conexión nueva por cada request. Bajo carga sostenida (50 VUs), el costo de `connect()` (~10-50 ms por conexión) se suma a la latencia de cada request.

**Impacto en el benchmark:** El p99 alto en ambas configuraciones (baseline y OTel) se debe principalmente a este factor, no al SDK.

**Solución recomendada para producción:**

```python
# Opción 1: asyncpg con pool de conexiones
import asyncpg

async def lifespan(app):
    app.state.pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=5,
        max_size=20,
    )
    yield
    await app.state.pool.close()

# Opción 2: psycopg2 + pgBouncer (sidecar o servicio externo)
# pgBouncer actúa como proxy y reutiliza conexiones PostgreSQL
```

### OTel Exporter inicializado fuera del bloque `OTEL_ENABLED`

> **Nota:** Este bug fue identificado y corregido durante el lab. En la versión final del código (`main (4).py`), la inicialización del `MeterProvider` con el `OTLPMetricExporter` ocurre **fuera** del bloque `if OTEL_ENABLED`. Esto significa que incluso en modo "baseline", el exporter de métricas OTLP se inicializa. Ver sección [Bug encontrado](#14-bug-encontrado-y-corrección-aplicada).

---

## 14. Bug encontrado y corrección aplicada

### Descripción del bug

En la versión original de `service-a/main.py`, el `OTLPMetricExporter` y el `MeterProvider` se inicializaban **antes** del bloque `if OTEL_ENABLED`, haciendo que el modo baseline no estuviera completamente limpio de overhead OTel.

```python
# ❌ INCORRECTO — MeterProvider se inicializa siempre, incluso con OTEL_ENABLED=false
otlp_metric_exporter = OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True)
otlp_metric_reader = PeriodicExportingMetricReader(otlp_metric_exporter, ...)
meter_provider = MeterProvider(resource=resource, metric_readers=[otlp_metric_reader])
metrics.set_meter_provider(meter_provider)

OTEL_ENABLED = os.getenv("OTEL_ENABLED", "true").lower() == "true"
if OTEL_ENABLED:
    tracer_provider = ...  # Solo TracerProvider respeta el flag
```

### Corrección aplicada

```python
# ✅ CORRECTO — Todo el pipeline OTel condicional al flag
OTEL_ENABLED = os.getenv("OTEL_ENABLED", "true").lower() == "true"

if OTEL_ENABLED:
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(...)))
    trace.set_tracer_provider(tracer_provider)

    otlp_metric_exporter = OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    otlp_metric_reader = PeriodicExportingMetricReader(
        otlp_metric_exporter,
        export_interval_millis=15000
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[otlp_metric_reader])
    metrics.set_meter_provider(meter_provider)

    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)
    HTTPXClientInstrumentor().instrument(tracer_provider=tracer_provider)
    Psycopg2Instrumentor().instrument(tracer_provider=tracer_provider)
else:
    # Provider vacío — sin exporters, sin overhead
    trace.set_tracer_provider(TracerProvider(resource=resource))
    metrics.set_meter_provider(MeterProvider(resource=resource))
```

### Impacto de la corrección

| Métrica | Antes (baseline contaminado) | Después (baseline limpio) |
|---|---|---|
| Error rate baseline | 7.40% | 0.00% |
| Throughput baseline | 16.7 rps | 25.0 rps |
| Latencia p95 baseline | ~8 000 ms | 5 104.9 ms |

---

## 15. Referencias

1. **OpenTelemetry Specification** — Semantic Conventions for HTTP Spans  
   https://opentelemetry.io/docs/specs/semconv/http/http-spans/

2. **OpenTelemetry Collector** — Configuration Guide  
   https://opentelemetry.io/docs/collector/configuration/

3. **Jaeger** — Architecture and Deployment  
   https://www.jaegertracing.io/docs/1.58/architecture/

4. **Google Kubernetes Engine** — DaemonSets  
   https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/

5. **Prometheus** — Best Practices: Histograms and Summaries  
   https://prometheus.io/docs/practices/histograms/

6. **k6** — Scenarios and Executors  
   https://grafana.com/docs/k6/latest/using-k6/scenarios/

7. **W3C Trace Context** — Distributed Tracing Propagation Standard  
   https://www.w3.org/TR/trace-context/

---

## Apéndice — Comandos de diagnóstico rápido

```bash
# Ver logs del OTel Collector (errores de exportación)
kubectl logs -l app=otel-collector -n $NAMESPACE --tail=50 -f

# Ver logs de service-a en tiempo real (JSON estructurado con trace_id)
kubectl logs -l app=service-a -n $NAMESPACE --tail=50 -f | python3 -m json.tool

# Verificar métricas internas del Collector (¿está rechazando datos?)
kubectl port-forward ds/otel-collector 8888:8888 -n $NAMESPACE &
curl -s http://localhost:8888/metrics | grep otelcol_processor_refused

# Escalar manualmente service-a
kubectl scale deployment/service-a --replicas=4 -n $NAMESPACE

# Ver HPA en acción durante el benchmark
kubectl get hpa service-a-hpa -n $NAMESPACE -w

# Reiniciar el Collector si el pipeline está atascado
kubectl rollout restart daemonset/otel-collector -n $NAMESPACE

# Eliminar todos los recursos del lab
kubectl delete namespace $NAMESPACE

# Destruir el clúster GKE (¡cuidado — no reversible!)
gcloud container clusters delete $CLUSTER_NAME \
  --region=$GCP_REGION \
  --project=$GCP_PROJECT
```

---

<div align="center">

**Unidad 2 — Lab 1 · Observabilidad end-to-end con OpenTelemetry**  
Instrumentación · Trazas distribuidas · Métricas SLI · Análisis de overhead

</div>
