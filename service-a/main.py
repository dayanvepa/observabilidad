"""
service-a: FastAPI — Orquestador principal
Recibe requests HTTP externos, hace llamadas a service-b y accede a PostgreSQL.
Instrumentado con OpenTelemetry (OTel) SDK: trazas, métricas y logs correlacionados.
"""

import logging
import os
import time
import psycopg2
from psycopg2 import pool
import httpx
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from pythonjsonlogger import jsonlogger

# ── OTel SDK: imports ────────────────────────────────────────────────────────
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.sdk.metrics._internal.exemplar.exemplar_filter import (
    TraceBasedExemplarFilter,
)

# ── Configuración desde variables de entorno ─────────────────────────────────
OTEL_ENABLED = os.getenv("OTEL_ENABLED", "true").lower() == "true"
OTEL_ENDPOINT   = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
SERVICE_B_URL   = os.getenv("SERVICE_B_URL", "http://service-b:8001")
DB_DSN          = os.getenv("DATABASE_URL", "postgresql://app:secret@postgres:5432/appdb")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9090"))
ENV             = os.getenv("ENVIRONMENT", "production")
APP_VERSION     = os.getenv("APP_VERSION", "1.0.0")

# ── 1. Resource: identidad del servicio en toda la telemetría ─────────────────
# El Resource viaja en cada señal (traza, métrica, log) para identificar el origen.
resource = Resource.create({
    SERVICE_NAME:    "service-a",
    SERVICE_VERSION: APP_VERSION,
    "deployment.environment": ENV,
    "cloud.provider": os.getenv("CLOUD_PROVIDER", "gcp"),
    "host.name":     os.getenv("HOSTNAME", "local"),
})

# ── 2. TracerProvider + OTLP exporter (gRPC → OTel Collector) ────────────────
if OTEL_ENABLED:
    tracer_provider = TracerProvider(resource=resource)
    otlp_span_exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    # BatchSpanProcessor: agrupa spans antes de enviar → reduce overhead de red
    tracer_provider.add_span_processor(BatchSpanProcessor(otlp_span_exporter))
    trace.set_tracer_provider(tracer_provider)
else:
    tracer_provider = TracerProvider(resource=resource)  # provider vacío, sin exporter

tracer = trace.get_tracer("service-a", APP_VERSION)


# ── 3. MeterProvider + Prometheus reader (scraping en :9090/metrics) ──────────
# PrometheusMetricReader expone las métricas OTel en formato Prometheus
if OTEL_ENABLED:
    from opentelemetry.sdk.metrics._internal.exemplar import TraceBasedExemplarFilter

    otlp_metric_exporter = OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    otlp_metric_reader   = PeriodicExportingMetricReader(
        otlp_metric_exporter,
        export_interval_millis=15000
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[otlp_metric_reader],
        exemplar_filter=TraceBasedExemplarFilter(),   # ← vincula métricas a trazas activas
    )
else:
    # NoOp real: MeterProvider sin readers → no exporta, no abre sockets, cero overhead
    meter_provider = MeterProvider(resource=resource)

metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("service-a", APP_VERSION)

# ── 4. Instrumentos de métricas (SLIs) ───────────────────────────────────────
http_requests_total = meter.create_counter(
    "http_requests_total",
    description="Total HTTP requests recibidos por service-a",
    unit="1",
)
http_request_duration = meter.create_histogram(
    "http_request_duration_seconds",
    description="Distribución de latencia de requests HTTP (p50/p95/p99)",
    unit="s",
)
db_query_duration = meter.create_histogram(
    "db_query_duration_seconds",
    description="Latencia de queries a PostgreSQL",
    unit="s",
)
service_b_calls_total = meter.create_counter(
    "service_b_calls_total",
    description="Llamadas HTTP a service-b",
    unit="1",
)
active_requests = meter.create_up_down_counter(
    "http_active_requests",
    description="Requests activos en vuelo (saturación)",
    unit="1",
)

# ── 5. Logging estructurado JSON con trace_id/span_id ────────────────────────
# El trace_id en el log es el PUENTE que correlaciona log ↔ traza en Grafana.
class OtelJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx and ctx.is_valid:
            # Formato hexadecimal de 32 dígitos para trace_id (estándar W3C)
            log_record["trace_id"] = format(ctx.trace_id, "032x")
            log_record["span_id"]  = format(ctx.span_id, "016x")
        log_record["service"]     = "service-a"
        log_record["version"]     = APP_VERSION
        log_record["environment"] = ENV

handler = logging.StreamHandler()
handler.setFormatter(OtelJsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("service-a")

# ── 6. Auto-instrumentación de librerías ─────────────────────────────────────
# HTTPX propaga automáticamente el header W3C traceparent
# hacia service-b.
if OTEL_ENABLED:
    HTTPXClientInstrumentor().instrument(tracer_provider=tracer_provider)
    Psycopg2Instrumentor().instrument(tracer_provider=tracer_provider)
# ── Conexión DB ───────────────────────────────────────────────────────────────
# ── Connection Pool PostgreSQL ────────────────────────────────────────────────
db_pool = pool.ThreadedConnectionPool(
    minconn=5,
    maxconn=50,
    dsn=DB_DSN,
)

def get_db_connection():
    return db_pool.getconn()

def release_db_connection(conn):
    if conn is not None:
        db_pool.putconn(conn)

# ── FastAPI App ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # No se necesita iniciar porque ya PrometheusmETRIC EN LINA 56 ya lo hizo en el serv
    # start_http_server(PROMETHEUS_PORT)
    logger.info("Prometheus metrics server started", extra={"port": PROMETHEUS_PORT})
    yield
    # Cerrar connection pool
    db_pool.closeall()
    # Shutdown: flushar todos los spans pendientes
    tracer_provider.shutdown()
    meter_provider.shutdown()

app = FastAPI(
    title="Service A",
    description="Microservicio orquestador — OTel end-to-end lab",
    version=APP_VERSION,
    lifespan=lifespan,
)

if OTEL_ENABLED:
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check — no genera trazas (excluido en el Collector)."""
    return {"status": "ok", "service": "service-a"}


@app.get("/order/{order_id}")
async def get_order(order_id: str, request: Request):
    """
    Flujo principal:

    1. Consulta PostgreSQL para obtener el pedido.
    2. Llama a service-b para obtener el inventario.
    3. Propaga el trace mediante W3C TraceContext.
    4. Retorna la información consolidada.
    """

    start = time.time()

    labels = {
        "endpoint": "/order",
        "method": "GET",
    }

    active_requests.add(1, labels)
    http_requests_total.add(1, labels)

    # Valor predeterminado para evitar que quede sin definir
    # si ocurre un error antes de la llamada a service-b.
    status_labels = {
        **labels,
        "status": "5xx",
    }

    # Span explícito para la lógica de negocio del pedido.
    with tracer.start_as_current_span(
        "order.get",
        kind=trace.SpanKind.INTERNAL,
        attributes={
            "http.method": "GET",
            "http.route": "/order/{order_id}",
            "order.id": order_id,
        },
    ) as order_span:

        # El span ya está activo aquí.
        active_requests.add(1, labels)
        http_requests_total.add(1, labels)

        span_context = order_span.get_span_context()
        trace_id = format(
            span_context.trace_id,
            "032x",
        )

        try:
            # ─────────────────────────────────────────────────────────────
            # Consulta del pedido en PostgreSQL
            # ─────────────────────────────────────────────────────────────

            with tracer.start_as_current_span(
                "fetch.order.db",
                kind=trace.SpanKind.CLIENT,
                attributes={
                    "db.system": "postgresql",
                    "db.operation": "SELECT",
                    "db.name": "appdb",
                    "order.id": order_id,
                },
            ) as db_span:

                db_start = time.time()
                conn = None
                cur = None

                try:
                    conn = get_db_connection()
                    cur = conn.cursor()

                    cur.execute(
                        """
                        SELECT id, product, quantity, status
                        FROM orders
                        WHERE id = %s
                        """,
                        (order_id,),
                    )

                    row = cur.fetchone()

                    db_duration = time.time() - db_start

                    db_query_duration.record(
                        db_duration,
                        {
                            "operation": "SELECT",
                            "table": "orders",
                        },
                    )

                    if not row:
                        db_span.set_status(
                            trace.StatusCode.ERROR,
                            "Order not found",
                        )

                        raise HTTPException(
                            status_code=404,
                            detail=f"Order {order_id} not found",
                        )

                    order_data = {
                        "id": row[0],
                        "product": row[1],
                        "quantity": row[2],
                        "status": row[3],
                    }

                    db_span.set_attribute(
                        "order.status",
                        order_data["status"],
                    )

                    logger.info(
                        "Order fetched from DB",
                        extra={
                            "order_id": order_id,
                            "status": order_data["status"],
                        },
                    )

                except HTTPException:
                    raise

                except Exception as e:
                    db_span.record_exception(e)
                    db_span.set_status(
                        trace.StatusCode.ERROR,
                        str(e),
                    )

                    logger.error(
                        "DB query failed",
                        extra={
                            "error": str(e),
                            "order_id": order_id,
                        },
                    )

                    raise HTTPException(
                        status_code=500,
                        detail="Database error",
                    )

                finally:
                    if cur is not None:
                        cur.close()

                    if conn is not None:
                        release_db_connection(conn)

            # ─────────────────────────────────────────────────────────────
            # Llamada a service-b
            # ─────────────────────────────────────────────────────────────

            with tracer.start_as_current_span(
                "call.service-b.inventory",
                kind=trace.SpanKind.CLIENT,
                attributes={
                    "http.method": "GET",
                    "peer.service": "service-b",
                    "order.product": order_data["product"],
                },
            ) as service_b_span:

                service_b_calls_total.add(
                    1,
                    {"status": "attempt"},
                )

                try:
                    async with httpx.AsyncClient(
                        timeout=5.0
                    ) as client:

                        # HTTPXClientInstrumentor inyecta automáticamente
                        # el header traceparent. No es necesario usar inject().
                        response = await client.get(
                            f"{SERVICE_B_URL}/inventory/"
                            f"{order_data['product']}"
                        )

                        response.raise_for_status()
                        inventory = response.json()

                    service_b_calls_total.add(
                        1,
                        {"status": "success"},
                    )

                    service_b_span.set_attribute(
                        "http.status_code",
                        response.status_code,
                    )

                    service_b_span.set_attribute(
                        "inventory.available",
                        inventory.get("available", 0),
                    )

                    status_labels = {
                        **labels,
                        "status": "2xx",
                    }

                    logger.info(
                        "Inventory fetched from service-b",
                        extra={
                            "product": order_data["product"],
                            "available": inventory.get("available"),
                        },
                    )

                except httpx.HTTPStatusError as e:
                    service_b_calls_total.add(
                        1,
                        {"status": "error"},
                    )

                    service_b_span.record_exception(e)
                    service_b_span.set_status(
                        trace.StatusCode.ERROR,
                        str(e),
                    )

                    status_labels = {
                        **labels,
                        "status": f"{e.response.status_code // 100}xx",
                    }

                    logger.error(
                        "service-b returned an HTTP error",
                        extra={
                            "error": str(e),
                            "status_code": e.response.status_code,
                        },
                    )

                    inventory = {
                        "available": -1,
                        "error": "service-b unavailable",
                    }

                except httpx.RequestError as e:
                    service_b_calls_total.add(
                        1,
                        {"status": "error"},
                    )

                    service_b_span.record_exception(e)
                    service_b_span.set_status(
                        trace.StatusCode.ERROR,
                        str(e),
                    )

                    status_labels = {
                        **labels,
                        "status": "5xx",
                    }

                    logger.error(
                        "Could not connect to service-b",
                        extra={
                            "error": str(e),
                        },
                    )

                    inventory = {
                        "available": -1,
                        "error": "service-b unavailable",
                    }

            # ─────────────────────────────────────────────────────────────
            # Métricas y respuesta
            # ─────────────────────────────────────────────────────────────

            total_duration = time.time() - start

            http_request_duration.record(
                total_duration,
                labels,
            )

            order_span.set_status(
                trace.StatusCode.OK
            )

            return {
                "order": order_data,
                "inventory": inventory,
                "trace_id": trace_id,
            }

        except HTTPException:
            order_span.set_status(
                trace.StatusCode.ERROR
            )
            raise

        except Exception as e:
            order_span.record_exception(e)
            order_span.set_status(
                trace.StatusCode.ERROR,
                str(e),
            )

            logger.error(
                "Order request failed",
                extra={
                    "error": str(e),
                    "order_id": order_id,
                },
            )

            raise HTTPException(
                status_code=500,
                detail="Order service error",
            )

        finally:
            active_requests.add(
                -1,
                labels,
            )


@app.get("/metrics/health")
async def metrics_health():
    """Estado del pipeline de telemetría."""
    return {
        "otel_enabled": OTEL_ENABLED,
        "otel_collector": OTEL_ENDPOINT,
        "prometheus_port": PROMETHEUS_PORT,
        "service_b_url": SERVICE_B_URL,
    }