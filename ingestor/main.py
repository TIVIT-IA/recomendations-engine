import asyncio
import asyncpg
import logging
import os
from dotenv import load_dotenv
from ingestor.core import configure_core, ingest_loop
from ingestor.tei_client import TEIClient

load_dotenv()

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='{"timestamp":"%(asctime)s","level":"%(levelname)s","message":%(message)s}'
    )

async def main():
    setup_logging()

    DATABASE_URL = os.getenv("DATABASE_URL")
    TEI_URL = os.getenv("TEI_URL")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "128"))
    CONCURRENCY = int(os.getenv("CONCURRENCY", "6"))
    TEI_MAX_BATCH = int(os.getenv("TEI_MAX_BATCH", "32"))
    TEI_TIMEOUT = int(os.getenv("TEI_TIMEOUT", "60"))

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurada")
    if not TEI_URL:
        raise RuntimeError("TEI_URL no configurada")

    configure_core(
        database_url=DATABASE_URL,
        tei_url=TEI_URL,
        batch_size=BATCH_SIZE,
        concurrency=CONCURRENCY,
        tei_max_batch=TEI_MAX_BATCH,
        tei_timeout=TEI_TIMEOUT,
    )

    pool = await asyncpg.create_pool(
    DATABASE_URL,
    statement_cache_size=0,
    max_cached_statement_lifetime=0,
    max_cacheable_statement_size=0
)

    tei_client = TEIClient(
        base_url=TEI_URL,
        max_batch=TEI_MAX_BATCH,
        timeout=TEI_TIMEOUT
    )

    try:
        await ingest_loop(pool, tei_client)  # type: ignore
    finally:
        await pool.close()  # type: ignore


if __name__ == "__main__":
    asyncio.run(main())
