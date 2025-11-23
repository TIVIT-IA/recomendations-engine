# ingestor/src/metrics.py
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client import Summary

BATCHES_PROCESSED = Counter("ingestor_batches_processed_total", "Total batches processed")
RECORDS_PROCESSED = Counter("ingestor_records_processed_total", "Total records processed")
BATCH_PROCESS_SECONDS = Histogram("ingestor_batch_process_seconds", "Seconds per batch")
TEI_CALLS = Counter("ingestor_tei_calls_total", "Total TEI calls")

def metrics_endpoint():
    from prometheus_client import generate_latest
    return generate_latest()
