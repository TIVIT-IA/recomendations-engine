# ingestor/core.py
"""
Core del ingestor: batching, generación de embeddings via TEI, y upsert a PostgreSQL (pgvector).
Se corrigieron/optimizron:
 - create_pool con statement_cache_size=0 (pgbouncer)
 - batching correcto: recorrer todo `data` en chunks de BATCH_SIZE
 - check previo en BD para evitar regenerar embeddings cuando hash_completo coincide
 - batch de upsert reducido a los items realmente necesarios
 - logging mejorado
 - semáforo y release robusto
"""

import asyncio
import aiohttp
import asyncpg
import json
import time
import logging
from typing import List, Any, Dict

from ingestor.tei_client import TEIClient
from ingestor.utils.identifier import extract_identifier_field
from ingestor.utils.text_unifier import build_texto_unificado
from ingestor.utils.hashing import compute_hash_completo, compute_hash_estable
from ingestor.sources.merge_sources import fetch_all_sources

logger = logging.getLogger("ingestor")
logger.setLevel(logging.INFO)

# Estas variables deben ser configuradas por main.py mediante configure_core(...)
DATABASE_URL: str
TEI_URL: str
BATCH_SIZE: int
CONCURRENCY: int
TEI_MAX_BATCH: int
TEI_TIMEOUT: int
EXPECTED_EMBEDDING_DIM: int = 384  # intfloat/e5-small -> 384


#############################################
# CONFIGURACIÓN DESDE main.py
#############################################

def configure_core(
    database_url: str,
    tei_url: str,
    batch_size: int = 8,
    concurrency: int = 4,
    tei_max_batch: int = 8,
    tei_timeout: int = 30,
    expected_embedding_dim: int = 384,
):
    global DATABASE_URL, TEI_URL, BATCH_SIZE, CONCURRENCY, TEI_MAX_BATCH, TEI_TIMEOUT, EXPECTED_EMBEDDING_DIM

    DATABASE_URL = database_url
    TEI_URL = tei_url
    BATCH_SIZE = batch_size
    CONCURRENCY = concurrency
    TEI_MAX_BATCH = tei_max_batch
    TEI_TIMEOUT = tei_timeout
    EXPECTED_EMBEDDING_DIM = expected_embedding_dim


#############################################
# SQL UPSERT
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


#############################################
# UTILIDADES
#############################################

def _embedding_to_pgvector_string(emb: List[float]) -> str:
    if not isinstance(emb, (list, tuple)):
        raise TypeError("Embedding debe ser lista/tupla de floats.")
    if len(emb) != EXPECTED_EMBEDDING_DIM:
        raise ValueError(f"Embedding length {len(emb)} != expected {EXPECTED_EMBEDDING_DIM}")
    return str([float(x) for x in emb])


async def upsert_batch(conn: asyncpg.Connection, batch_items: List[Dict[str, Any]], embeddings: List[str]):
    # Usamos transaction para atomicidad
    async with conn.transaction():
        for it, emb_str in zip(batch_items, embeddings):
            await conn.execute(
                UPSERT_SQL,
                it["id_estable"],
                it["hash_completo"],
                json.dumps(it["json_data"]),
                it["texto_unificado"],
                emb_str,
            )


#############################################
# PROCESAR UN BATCH
#############################################

async def process_batch(session: aiohttp.ClientSession, pool: asyncpg.Pool, tei_client: TEIClient,
                        batch_items: List[Dict[str, Any]], texts: List[str], sem: asyncio.Semaphore):

    start = time.time()

    try:
        max_attempts = 3
        attempt = 0
        embeddings = None
        last_exc = None

        while attempt < max_attempts:
            try:
                embeddings = await asyncio.wait_for(
                    tei_client.embed_batch(session, texts),
                    timeout=TEI_TIMEOUT
                )
                break
            except Exception as e:
                last_exc = e
                attempt += 1
                backoff = 0.5 * (2 ** (attempt - 1))
                logger.warning(json.dumps({
                    "event": "tei_retry",
                    "attempt": attempt,
                    "error": str(e),
                    "backoff_s": backoff
                }))
                await asyncio.sleep(backoff)

        if embeddings is None:
            raise RuntimeError(f"TEI failed after {max_attempts} attempts: {last_exc}")

        if not isinstance(embeddings, list) or len(embeddings) != len(batch_items):
            raise RuntimeError("TEI returned embeddings in unexpected format or length mismatch.")

        embeddings_pg = []
        for emb in embeddings:
            emb_pg = _embedding_to_pgvector_string(emb)
            embeddings_pg.append(emb_pg)

        async with pool.acquire() as conn:
            await upsert_batch(conn, batch_items, embeddings_pg)

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
        try:
            sem.release()
        except Exception:
            pass


#############################################
# INFINITE INGEST LOOP (mejorado)
#############################################

async def ingest_loop(pool: asyncpg.Pool, tei_client: TEIClient):
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        logger.info(json.dumps({"event": "ingestor_started"}))

        while True:
            try:
                data = await fetch_all_sources()

                if not data:
                    await asyncio.sleep(1)
                    continue

                # Procesar en chunks reales
                for i in range(0, len(data), BATCH_SIZE):
                    chunk = data[i:i + BATCH_SIZE]

                    # Preparar listas y metadatos
                    batch_items = []
                    texts = []
                    ids = []

                    for wrapper in chunk:
                        record = wrapper.get("raw") or {}

                        id_estable = extract_identifier_field(record, "email")
                        hcomp = compute_hash_completo(record)
                        texto = build_texto_unificado(record) or " "

                       

                        batch_items.append({
                            "id_estable": id_estable,
                            "hash_completo": hcomp,
                            "json_data": record,
                            "texto_unificado": texto
                        })

                        texts.append(texto)
                        ids.append(id_estable)

                    # Consultar la BD para saber cuáles ya existen y sus hashes
                    async with pool.acquire() as conn:
                        if ids:
                            rows = await conn.fetch(
                                "SELECT id_estable, hash_completo FROM trabajadores WHERE id_estable = ANY($1)",
                                ids
                            )
                        else:
                            rows = []

                    existing_map = {r["id_estable"]: r["hash_completo"] for r in rows}

                    # Filtrar batch_items: si existe y hash_completo igual -> skip
                    to_process_items = []
                    to_process_texts = []

                    for it, txt in zip(batch_items, texts):
                        existing_hash = existing_map.get(it["id_estable"])
                        if existing_hash is not None and existing_hash == it["hash_completo"]:
                            # ya existe y no cambió -> ignorar
                            continue
                        to_process_items.append(it)
                        to_process_texts.append(txt)

                    if not to_process_items:
                        # nada que procesar en este chunk
                        continue

                    await sem.acquire()

                    # Crear tarea para procesar este batch (no bloquear loop)
                    asyncio.create_task(
                        process_batch(session, pool, tei_client, to_process_items, to_process_texts, sem)
                    )

                # fin for chunks

            except Exception as e:
                logger.error(json.dumps({
                    "event": "loop_error",
                    "error": str(e)
                }))
                await asyncio.sleep(2)


#############################################
# INICIAR INGESTOR
#############################################

async def start_ingestor():
    if not DATABASE_URL or not TEI_URL:
        raise RuntimeError("configure_core must be called before start_ingestor()")

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=max(2, CONCURRENCY * 2),
        statement_cache_size=0,
        max_cached_statement_lifetime=0,
        max_cacheable_statement_size=0
    )

    tei_client = TEIClient(TEI_URL, max_batch=TEI_MAX_BATCH)

    await ingest_loop(pool, tei_client)
