# Base image with common settings
FROM python:3.13-slim-trixie AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    ca-certificates wget netcat-openbsd \
    libeccodes0 libeccodes-data \
    libnetcdf22 libhdf5-310 \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY docker-scripts/wait-for-services.sh /usr/local/bin/
COPY docker-scripts/run-tests.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh

# Stage for building the project and compiling dependencies
FROM base AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

# Test stage
FROM base AS tests
COPY src/ ./src/
COPY tests/ ./tests/
# COPY src/ tests/ ./
ENV PYTHONPATH=/app
ENTRYPOINT ["run-tests.sh"]

# Runtime stage
FROM base AS runtime
COPY src/ ./src/
ENV PYTHONPATH=/app

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8000/health || exit 1

CMD ["uvicorn", "--app-dir", "./src/", "main:app", "--host", "0.0.0.0", "--port", "8000"]
