# ingestor/src/sources/merge_sources.py
import os
import asyncio
import logging
from typing import List, Dict, Any
from aiolimiter import AsyncLimiter

from ingestor.sources.impl.drive_source import DriveSource
from ingestor.sources.impl.generic_api import GenericAPISource

logger = logging.getLogger("merge_sources")

# Rate limit global por dominio (ajusta según tus APIs)
GLOBAL_RATE = int(os.getenv("SOURCES_RATE_PER_SEC", "10"))
limiter = AsyncLimiter(max_rate=GLOBAL_RATE, time_period=1)

# Timeouts / retries
FETCH_TIMEOUT = int(os.getenv("SOURCE_FETCH_TIMEOUT", "20"))
MAX_RETRIES = int(os.getenv("SOURCE_MAX_RETRIES", "3"))

async def _safe_fetch(source):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with limiter:
                items = await asyncio.wait_for(source.fetch(), timeout=FETCH_TIMEOUT)
            # Normaliza lista
            if not isinstance(items, list):
                return []
            return items
        except asyncio.TimeoutError:
            logger.warning(f"[WARN] timeout fetching {getattr(source, 'url', getattr(source, 'folder_id', 'unknown'))} attempt={attempt}")
        except Exception as e:
            logger.warning(f"[WARN] error fetching from {getattr(source, 'url', getattr(source, 'folder_id', 'unknown'))}: {e} attempt={attempt}")
        await asyncio.sleep(0.5 * attempt)
    return []

async def fetch_all_sources() -> List[Dict[str, Any]]:
    """
    Crea y agrupa todas las fuentes configuradas por ENV.
    Devuelve lista de wrappers: {"raw": {...}, "source": "..."}
    """
    api1_url = os.getenv("API1_URL")
    api2_url = os.getenv("API2_URL")
    drive_folder = os.getenv("DRIVE_FOLDER_ID")

    sources = []
    if api1_url:
        sources.append(GenericAPISource(api1_url))
    if api2_url:
        sources.append(GenericAPISource(api2_url))
    if drive_folder:
        sources.append(DriveSource(drive_folder))

    # Ejecutar fetches concurrentes (pero rateados internamente)
    tasks = [asyncio.create_task(_safe_fetch(s)) for s in sources]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    merged = []
    for src, items in zip(sources, results):
        if items:
            # añadir campo source para trazabilidad
            for r in items:
                # r ya debería tener 'raw', si no, normalizamos
                if isinstance(r, dict) and "raw" in r:
                    wrapper = r
                else:
                    wrapper = {"raw": r, "source": getattr(src, "url", getattr(src, "folder_id", "unknown"))}
                merged.append(wrapper)

    # deduplicate by stable id candidate (try to pick dni/email if present)
    seen = set()
    deduped = []
    for w in merged:
        raw = w.get("raw", {})
        key = (str(raw.get("dni") or raw.get("correo") or raw.get("id") or raw.get("documento") or json.dumps(raw, sort_keys=True)))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(w)

    logger.info(f"[merge_sources] fetched {len(merged)} items -> deduped {len(deduped)}")
    return deduped
