FROM python:3.11-slim

WORKDIR /app

# system deps
RUN apt-get update && apt-get install -y gcc libpq-dev build-essential libmagic1 poppler-utils tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY ingestor/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy project (assume structure)
COPY ingestor /app

EXPOSE 9001  # health/metrics port

# Entrypoint: run health server in background + worker
CMD ["sh", "-c", "python src/health_server.py & python src/ingest_worker.py"]
