# ingestor/src/ingest_worker.py
import asyncio
import aiohttp
import asyncpg
import os
import json
import time
import logging
import backoff
from dotenv import load_dotenv
from ingestor.services.tei_client import TEIClient
from ingestor.utils.text_unifier import build_texto_unificado
from ingestor.utils.hashing import compute_hash_completo, compute_hash_estable
from ingestor.sources.merge_sources import fetch_all_sources


#############################################
# CONFIG & LOGGING
#############################################

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
TEI_URL = os.getenv("TEI_URL")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "128"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "6"))
TEI_MAX_BATCH = int(os.getenv("TEI_MAX_BATCH", "32"))
TEI_TIMEOUT = int(os.getenv("TEI_TIMEOUT", "60"))

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no configurada en .env")
if not TEI_URL:
    raise RuntimeError("TEI_URL no configurada en .env")

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","message":%(message)s}'
)
logger = logging.getLogger("ingestor")


#############################################
# DB UPSERT
#############################################

UPSERT_SQL = """
INSERT INTO trabajadores (id_estable, hash_completo, json_data, texto_unificado, embedding, updated_at)
VALUES ($1, $2, $3::jsonb, $4, $5::vector, now())
ON CONFLICT (id_estable) DO UPDATE
SET hash_completo = EXCLUDED.hash_completo,
    json_data = EXCLUDED.json_data,
    texto_unificado = EXCLUDED.texto_unificado,
    embedding = EXCLUDED.embedding,
    updated_at = now();
"""


async def upsert_batch(conn: asyncpg.Connection, batch_items, embeddings):
    async with conn.transaction():
        for it, emb in zip(batch_items, embeddings):
            await conn.execute(
                UPSERT_SQL,
                it["id_estable"],
                it["hash_completo"],
                json.dumps(it["json_data"]),
                it["texto_unificado"],
                emb
            )


#############################################
# PROCESSING A SINGLE BATCH
#############################################

async def process_batch(session, pool, tei_client, batch_items, texts, sem):
    start = time.time()

    try:
        embeddings = await tei_client.embed_batch(session, texts)

        async with pool.acquire() as conn:
            await upsert_batch(conn, batch_items, embeddings)

        took = round(time.time() - start, 2)
        logger.info(json.dumps({
            "event": "batch_processed",
            "records": len(batch_items),
            "time_seconds": took
        }))

    except Exception as e:
        logger.error(json.dumps({
            "event": "batch_error",
            "error": str(e),
            "records": len(batch_items)
        }))

    finally:
        sem.release()


#############################################
# MAIN INGEST LOOP
#############################################

async def ingest_loop(pool: asyncpg.Pool, tei_client: TEIClient):
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        logger.info('{"event":"ingestor_started"}')

        while True:
            try:
                data = await fetch_all_sources()

                if not data:
                    await asyncio.sleep(1)
                    continue

                batch_items = []
                texts = []

                # build normalized records
                for wrapper in data[:BATCH_SIZE]:
                    record = wrapper.get("raw") or {}

                    id_estable = compute_hash_estable(record)
                    hcomp = compute_hash_completo(record)
                    texto = build_texto_unificado(record) or " "

                    batch_items.append({
                        "id_estable": id_estable,
                        "hash_completo": hcomp,
                        "json_data": record,
                        "texto_unificado": texto
                    })

                    texts.append(texto)

                await sem.acquire()

                asyncio.create_task(
                    process_batch(session, pool, tei_client, batch_items, texts, sem)
                )

            except Exception as e:
                logger.error(json.dumps({
                    "event": "loop_error",
                    "error": str(e)
                }))
                await asyncio.sleep(2)


#############################################
# MAIN
#############################################

async def main():
    pool = await asyncpg.create_pool(DATABASE_URL)

    try:
        tei_client = TEIClient(
            base_url=TEI_URL,
            max_batch=TEI_MAX_BATCH,
            timeout=TEI_TIMEOUT
        )

        await ingest_loop(pool, tei_client)

    finally:
        await pool.close()
        logger.info('{"event":"ingestor_stopped"}')


if __name__ == "__main__":
    asyncio.run(main())
