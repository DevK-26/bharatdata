# One image for both Python services: `api` (uvicorn) and `ingest` (celery).
# docker-compose.yml picks the command per service.
FROM python:3.12-slim
WORKDIR /app

# Install dependencies first so code edits don't bust the pip layer.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

# The registry is read at runtime (BHARATDATA_REGISTRY_DIR, default ./registry).
COPY registry ./registry

# Never run as root inside the container.
RUN useradd --create-home --uid 1000 bharat && mkdir -p /data/raw && chown bharat /data/raw
USER bharat

EXPOSE 8000
CMD ["uvicorn", "bharatdata.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
