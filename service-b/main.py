"""
service-b: FastAPI — Servicio de inventario

Recibe llamadas de service-a, consulta inventario en PostgreSQL
y continúa el trace distribuido mediante W3C TraceContext.
"""

import logging
import os
import random
import time
from contextlib import asynccontextmanager

import psycopg2
from fastapi import FastAPI, HTTPException
from pythonjsonlogger import jsonlogger

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import (
    Resource,
    SERVICE_NAME,
    SERVICE_VERSION,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


# ─────────────────────────────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────────────────────────────

OTEL_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector:4317",
)

DB_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://app:secret@postgres:5432/appdb",
)

PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9091"))
ENV = os.getenv("ENVIRONMENT", "production")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")


# ─────────────────────────────────────────────────────────────────────────────
# Resource: identidad de service-b
# ─────────────────────────────────────────────────────────────────────────────

resource = Resource.create(
    {
        SERVICE_NAME: "service-b",
        SERVICE_VERSION: APP_VERSION,
        "deployment.environment": ENV,
        "cloud.provider": os.getenv("CLOUD_PROVIDER", "gcp"),
    }
)

# ─────────────────────────────────────────────────────────────────────────────
# Tracing
# ─────────────────────────────────────────────────────────────────────────────

OTEL_ENABLED = os.getenv("OTEL_ENABLED", "true").lower() == "true"

if OTEL_ENABLED:
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True))
    )
    trace.set_tracer_provider(tracer_provider)
else:
    tracer_provider = TracerProvider(resource=resource)  # provider vacío, sin exporter

tracer = trace.get_tracer("service-a", APP_VERSION)


# ─────────────────────────────────────────────────────────────────────────────
# Métricas
# ─────────────────────────────────────────────────────────────────────────────

otlp_metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(
        endpoint=OTEL_ENDPOINT,
        insecure=True,
    ),
    export_interval_millis=15000,
)

meter_provider = MeterProvider(
    resource=resource,
    metric_readers=[otlp_metric_reader],
)

metrics.set_meter_provider(meter_provider)

meter = metrics.get_meter(
    "service-b",
    APP_VERSION,
)


inventory_requests = meter.create_counter(
    "inventory_requests_total",
    description="Total de consultas de inventario procesadas",
    unit="1",
)

inventory_query_duration = meter.create_histogram(
    "inventory_query_duration_seconds",
    description="Latencia de consultas de inventario a PostgreSQL",
    unit="s",
)

cache_hits = meter.create_counter(
    "inventory_cache_hits_total",
    description="Cantidad de consultas atendidas desde la caché",
    unit="1",
)


# ─────────────────────────────────────────────────────────────────────────────
# Logging JSON con trace_id y span_id
# ─────────────────────────────────────────────────────────────────────────────

class OtelJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)

        current_span = trace.get_current_span()
        span_context = current_span.get_span_context()

        if span_context.is_valid:
            log_record["trace_id"] = format(
                span_context.trace_id,
                "032x",
            )
            log_record["span_id"] = format(
                span_context.span_id,
                "016x",
            )

        log_record["service"] = "service-b"
        log_record["version"] = APP_VERSION
        log_record["environment"] = ENV


handler = logging.StreamHandler()
handler.setFormatter(
    OtelJsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
)

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
)

logger = logging.getLogger("service-b")


# ─────────────────────────────────────────────────────────────────────────────
# Instrumentación de Psycopg2
# ─────────────────────────────────────────────────────────────────────────────

if OTEL_ENABLED:
    Psycopg2Instrumentor().instrument(tracer_provider=tracer_provider)


# ─────────────────────────────────────────────────────────────────────────────
# Caché en memoria
# ─────────────────────────────────────────────────────────────────────────────

_inventory_cache: dict[str, dict] = {}


def get_db_connection():
    return psycopg2.connect(DB_DSN)


def get_current_trace_id() -> str:
    """
    Obtiene el trace_id del span HTTP activo creado por FastAPIInstrumentor.
    """
    current_span = trace.get_current_span()
    span_context = current_span.get_span_context()

    if not span_context.is_valid:
        return "00000000000000000000000000000000"

    return format(
        span_context.trace_id,
        "032x",
    )


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "Service-b started",
        extra={
            "prometheus_port": PROMETHEUS_PORT,
            "otel_endpoint": OTEL_ENDPOINT,
        },
    )

    yield

    tracer_provider.shutdown()
    meter_provider.shutdown()


app = FastAPI(
    title="Service B",
    description="Microservicio de inventario — OTel end-to-end lab",
    version=APP_VERSION,
    lifespan=lifespan,
)


# IMPORTANTE:
# La instrumentación se registra después de crear la instancia app.
# Esto permite extraer el header W3C traceparent recibido desde service-a.
if OTEL_ENABLED:
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "service-b",
    }


@app.get("/inventory/{product_id}")
async def get_inventory(product_id: str):
    """
    Retorna la disponibilidad de inventario.

    Cuando la solicitud proviene de service-a, FastAPIInstrumentor:
    1. Extrae el header traceparent.
    2. Continúa el trace iniciado por service-a.
    3. Crea el span servidor de service-b.
    """

    start = time.time()
    trace_id = get_current_trace_id()

    inventory_requests.add(
        1,
        {"product": product_id},
    )

    logger.info(
        "Inventory request received",
        extra={
            "product_id": product_id,
            "trace_id": trace_id,
        },
    )

    # ── Consulta a la caché ────────────────────────────────────────────────
    if product_id in _inventory_cache:
        with tracer.start_as_current_span(
            "inventory.cache.hit",
            attributes={
                "cache.type": "in-memory",
                "product.id": product_id,
            },
        ) as cache_span:
            cache_hits.add(
                1,
                {"product": product_id},
            )

            cache_span.set_attribute(
                "inventory.cache_hit",
                True,
            )

            logger.info(
                "Inventory returned from cache",
                extra={
                    "product_id": product_id,
                    "trace_id": trace_id,
                },
            )

            return {
                **_inventory_cache[product_id],
                "trace_id": trace_id,
            }

    # ── Consulta a PostgreSQL ─────────────────────────────────────────────
    with tracer.start_as_current_span(
        "inventory.db.fetch",
        kind=trace.SpanKind.CLIENT,
        attributes={
            "db.system": "postgresql",
            "db.operation": "SELECT",
            "db.name": "appdb",
            "product.id": product_id,
        },
    ) as span:
        conn = None
        cur = None

        try:
            db_start = time.time()

            conn = get_db_connection()
            cur = conn.cursor()

            # Simulación de latencia variable de la base de datos.
           # time.sleep(
            #    random.uniform(0.01, 0.09)
           # )

            cur.execute(
                """
                SELECT product_id, available, warehouse, last_updated
                FROM inventory
                WHERE product_id = %s
                """,
                (product_id,),
            )

            row = cur.fetchone()

            duration = time.time() - db_start

            inventory_query_duration.record(
                duration,
                {"operation": "SELECT"},
            )

            if not row:
                span.set_status(
                    trace.StatusCode.ERROR,
                    "Product not found",
                )

                raise HTTPException(
                    status_code=404,
                    detail=f"Product {product_id} not found",
                )

            result = {
                "product_id": row[0],
                "available": row[1],
                "warehouse": row[2],
                "last_updated": str(row[3]),
            }

            span.set_attribute(
                "inventory.available",
                result["available"],
            )

            span.set_attribute(
                "inventory.warehouse",
                result["warehouse"],
            )

            span.set_status(
                trace.StatusCode.OK
            )

            _inventory_cache[product_id] = result

            logger.info(
                "Inventory fetched from database",
                extra={
                    "product_id": product_id,
                    "available": result["available"],
                    "duration_s": round(duration, 4),
                    "trace_id": trace_id,
                },
            )

            return {
                **result,
                "trace_id": trace_id,
            }

        except HTTPException:
            raise

        except Exception as e:
            span.record_exception(e)
            span.set_status(
                trace.StatusCode.ERROR,
                str(e),
            )

            logger.error(
                "Inventory database query failed",
                extra={
                    "error": str(e),
                    "product_id": product_id,
                    "trace_id": trace_id,
                },
            )

            raise HTTPException(
                status_code=500,
                detail="Inventory service error",
            )

        finally:
            if cur is not None:
                cur.close()

            if conn is not None:
                conn.close()


@app.post("/inventory/{product_id}/reserve")
async def reserve_inventory(
    product_id: str,
    quantity: int = 1,
):
    """
    Reserva unidades de inventario y muestra spans anidados.
    """

    with tracer.start_as_current_span(
        "inventory.business.reserve",
        attributes={
            "product.id": product_id,
            "reservation.units": quantity,
        },
    ) as span:

        trace_id = get_current_trace_id()

        logger.info(
            "Reserving inventory",
            extra={
                "product_id": product_id,
                "quantity": quantity,
                "trace_id": trace_id,
            },
        )

        with tracer.start_as_current_span(
            "inventory.validate.stock"
        ) as validation_span:

            time.sleep(
                random.uniform(0.005, 0.02)
            )

            available = random.randint(0, 100)

            validation_span.set_attribute(
                "stock.available",
                available,
            )

            if available < quantity:
                validation_span.set_status(
                    trace.StatusCode.ERROR,
                    "Insufficient stock",
                )

                span.set_status(
                    trace.StatusCode.ERROR,
                    "Reservation failed",
                )

                raise HTTPException(
                    status_code=409,
                    detail="Insufficient stock",
                )

        span.set_attribute(
            "reservation.approved",
            True,
        )

        span.set_status(
            trace.StatusCode.OK
        )

        _inventory_cache.pop(
            product_id,
            None,
        )

        return {
            "reserved": quantity,
            "product_id": product_id,
            "status": "confirmed",
            "trace_id": trace_id,
        }