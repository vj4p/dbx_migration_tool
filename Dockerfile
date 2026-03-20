# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libxml2-dev \
        libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="dbx_migration_tool" \
      org.opencontainers.image.description="Informatica PowerCenter → Databricks ETL migration tool" \
      org.opencontainers.image.vendor="CDER" \
      org.opencontainers.image.base.name="python:3.11-slim"

# Runtime system deps (libxml2 for lxml at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libxml2 \
        libxslt1.1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — required for FedRAMP / ECS hardening
RUN groupadd --gid 1001 appgroup \
 && useradd  --uid 1001 --gid appgroup --no-create-home --shell /sbin/nologin appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY src/       ./src/
COPY notebooks/ ./notebooks/
COPY resources/ ./resources/

# Ensure Python can find the package
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# AWS region default (override via ECS task env or Secrets Manager)
ENV AWS_DEFAULT_REGION=us-gov-west-1

USER appuser

# Health check — simple import smoke test
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from src.migration.parser import InformaticaXMLParser; print('ok')" || exit 1

# Default entrypoint: run the CLI
ENTRYPOINT ["python", "-m", "src.migration.cli"]
CMD ["--help"]
