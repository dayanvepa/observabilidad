"""
service-b: FastAPI — Microservicio de inventario
Expone /inventory/{product_id}, consulta PostgreSQL y responde al orquestador.
Instrumentado con OpenTelemetry (OTel) SDK: trazas, métricas y logs correlacionados.
Métricas exportadas por OTLP push al Collector. No expone /metrics propio.
"""

import logging
import os
import time
import psycopg2
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pythonjsonlogger import jsonlogger

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor


OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
DB_DSN        = os.getenv("DATABASE_URL", "postgresql://app:secret@postgres:5432/appdb")
ENV           = os.getenv("ENVIRONMENT", "production")
APP_VERSION   = os.getenv("APP_VERSION", "1.0.0")
OTEL_ENABLED  = os.getenv("OTEL_ENABLED", "true").lower() == "true"

resource = Resource.create({
    SERVICE_NAME:    "service-b",
    SERVICE_VERSION: APP_VERSION,
    "deployment.environment": ENV,
    "cloud.provider": os.getenv("CLOUD_PROVIDER", "gcp"),
    "host.name":     os.getenv("HOSTNAME", "local"),
})

meter = None
meter_provider = None
tracer_provider = TracerProvider(resource=resource)

if OTEL_ENABLED:
    otlp_span_exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    tracer_provider.add_span_processor(BatchSpanProcessor(otlp_span_exporter))

    otlp_metric_exporter = OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True)
    otlp_metric_reader = PeriodicExportingMetricReader(otlp_metric_exporter, export_interval_millis=15000)
    meter_provider = MeterProvider(resource=resource, metric_readers=[otlp_metric_reader])
    metrics.set_meter_provider(meter_provider)
    meter = metrics.get_meter("service-b", APP_VERSION)

trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer("service-b", APP_VERSION)

if meter:
    http_requests_total = meter.create_counter(
        "http_requests_total",
        description="Total HTTP requests recibidos por service-b",
        unit="1",
    )
    http_request_duration = meter.create_histogram(
        "http_request_duration_seconds",
        description="Distribución de latencia de requests HTTP",
        unit="s",
    )
    db_query_duration = meter.create_histogram(
        "db_query_duration_seconds",
        description="Latencia de queries a PostgreSQL",
        unit="s",
    )
    active_requests = meter.create_up_down_counter(
        "http_active_requests",
        description="Requests activos en vuelo",
        unit="1",
    )


class OtelJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx and ctx.is_valid:
            log_record["trace_id"] = format(ctx.trace_id, "032x")
            log_record["span_id"]  = format(ctx.span_id, "016x")
        log_record["service"]     = "service-b"
        log_record["version"]     = APP_VERSION
        log_record["environment"] = ENV

handler = logging.StreamHandler()
handler.setFormatter(OtelJsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("service-b")

if OTEL_ENABLED:
    Psycopg2Instrumentor().instrument(tracer_provider=tracer_provider)


def get_db_connection():
    return psycopg2.connect(DB_DSN)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Service started", extra={"otel_enabled": OTEL_ENABLED})
    yield
    tracer_provider.shutdown()
    if meter_provider:
        meter_provider.shutdown()


app = FastAPI(
    title="Service B",
    description="Microservicio de inventario — OTel end-to-end lab",
    version=APP_VERSION,
    lifespan=lifespan,
)

if OTEL_ENABLED:
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)


@app.get("/health")
async def health():
    """Health check — no genera trazas (excluido en el Collector)."""
    return {"status": "ok", "service": "service-b"}


@app.get("/inventory/{product_id}")
async def get_inventory(product_id: str):
    """
    Consulta el inventario de un producto en PostgreSQL.
    El trace_id de la traza activa se usa para correlación con logs.
    """
    start = time.time()
    labels = {"endpoint": "/inventory/{product_id}", "method": "GET"}

    if meter:
        active_requests.add(1, labels)
        http_requests_total.add(1, labels)

    with tracer.start_as_current_span(
        "inventory.get",
        kind=trace.SpanKind.INTERNAL,
        attributes={
            "http.method": "GET",
            "http.route": "/inventory/{product_id}",
            "product.id": product_id,
        },
    ) as inventory_span:
        span_context = inventory_span.get_span_context()
        trace_id = format(span_context.trace_id, "032x")

        try:
            with tracer.start_as_current_span(
                "fetch.inventory.db",
                kind=trace.SpanKind.CLIENT,
                attributes={
                    "db.system": "postgresql",
                    "db.operation": "SELECT",
                    "db.name": "appdb",
                    "product.id": product_id,
                },
            ) as db_span:
                db_start = time.time()
                conn = None
                cur = None
                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT product_id, available, warehouse, last_updated "
                        "FROM inventory WHERE product_id = %s",
                        (product_id,),
                    )
                    row = cur.fetchone()

                    db_duration = time.time() - db_start
                    if meter:
                        db_query_duration.record(db_duration, {"operation": "SELECT", "table": "inventory"})

                    if not row:
                        db_span.set_status(trace.StatusCode.ERROR, "Product not found")
                        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")

                    inventory = {
                        "product_id": row[0],
                        "available": row[1],
                        "warehouse": row[2],
                        "last_updated": row[3].isoformat() if row[3] else None,
                    }

                    db_span.set_attribute("inventory.available", inventory["available"])
                    logger.info("Inventory fetched from DB", extra={
                        "product_id": product_id,
                        "available": inventory["available"],
                    })

                except HTTPException:
                    raise

                except Exception as e:
                    db_span.record_exception(e)
                    db_span.set_status(trace.StatusCode.ERROR, str(e))
                    logger.error("DB query failed", extra={"error": str(e), "product_id": product_id})
                    raise HTTPException(status_code=500, detail="Database error")

                finally:
                    if cur is not None:
                        cur.close()
                    if conn is not None:
                        conn.close()

            total_duration = time.time() - start
            if meter:
                http_request_duration.record(total_duration, labels)

            inventory_span.set_status(trace.StatusCode.OK)

            return {
                **inventory,
                "trace_id": trace_id,
            }

        except HTTPException:
            inventory_span.set_status(trace.StatusCode.ERROR)
            raise

        except Exception as e:
            inventory_span.record_exception(e)
            inventory_span.set_status(trace.StatusCode.ERROR, str(e))
            logger.error("Inventory request failed", extra={"error": str(e), "product_id": product_id})
            raise HTTPException(status_code=500, detail="Inventory service error")

        finally:
            if meter:
                active_requests.add(-1, labels)


@app.get("/telemetry/health")
async def telemetry_health():
    """Estado del pipeline de telemetría."""
    return {
        "otel_enabled": OTEL_ENABLED,
        "otel_collector": OTEL_ENDPOINT,
    }
