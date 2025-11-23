# ingestor/src/sources/merge_sources.py
import os
import json
import asyncio
import logging
from typing import List, Dict, Any
from aiolimiter import AsyncLimiter
from dotenv import load_dotenv

from ingestor.sources.impl.drive_source import DriveSource
from ingestor.sources.impl.generic_api import GenericAPISource

logger = logging.getLogger("merge_sources")

# ===============================================
# CARGAR .env NORMALMENTE
# ===============================================
# Igual que en main.py: carga el .env desde el directorio donde ejecutes python.
load_dotenv()

# ===============================================
# CONFIGURACIÓN GLOBAL
# ===============================================
GLOBAL_RATE = int(os.getenv("SOURCES_RATE_PER_SEC", "10"))
limiter = AsyncLimiter(max_rate=GLOBAL_RATE, time_period=1)

FETCH_TIMEOUT = int(os.getenv("SOURCE_FETCH_TIMEOUT", "20"))
MAX_RETRIES = int(os.getenv("SOURCE_MAX_RETRIES", "3"))

# ===============================================
# API KEY — Leída desde .env
# ===============================================
API_KEY = os.getenv("API_KEY")
logger.info(f"[merge_sources] API_KEY leída desde .env: {API_KEY}")

if not API_KEY:
    logger.warning("API_KEY NO configurada — Supabase responderá 401")
    SUPABASE_HEADERS = {}
else:
    SUPABASE_HEADERS = {
        "apikey": API_KEY,
        "Authorization": f"Bearer {API_KEY}",
    }

logger.info(f"[merge_sources] usando headers: {SUPABASE_HEADERS}")

# ===============================================
# SAFE FETCH (con timeout + retries)
# ===============================================
async def _safe_fetch(source):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with limiter:
                items = await asyncio.wait_for(source.fetch(), timeout=FETCH_TIMEOUT)

            return items if isinstance(items, list) else []

        except asyncio.TimeoutError:
            logger.warning(
                f"[WARN] timeout fetching "
                f"{getattr(source, 'url', getattr(source, 'folder_id', 'unknown'))}, "
                f"attempt={attempt}"
            )

        except Exception as e:
            logger.warning(
                f"[WARN] error fetching from "
                f"{getattr(source, 'url', getattr(source, 'folder_id', 'unknown'))}: "
                f"{e} attempt={attempt}"
            )

        await asyncio.sleep(0.5 * attempt)

    return []

# ===============================================
# FUNCIÓN PRINCIPAL
# ===============================================
async def fetch_all_sources() -> List[Dict[str, Any]]:
    """
    Retorna todos los datos combinados de todas las fuentes:
    [{"raw": {...}, "source": "..."}]
    """

    api1_url = os.getenv("API1_URL")
    api2_url = os.getenv("API2_URL")
    drive_folder = os.getenv("DRIVE_FOLDER_ID")

    sources = []

    # Fuentes Supabase
    if api1_url:
        sources.append(GenericAPISource(api1_url, headers=SUPABASE_HEADERS))

    if api2_url:
        sources.append(GenericAPISource(api2_url, headers=SUPABASE_HEADERS))

    # Google Drive
    if drive_folder:
        sources.append(DriveSource(drive_folder))

    # Ejecutar concurrentemente
    tasks = [asyncio.create_task(_safe_fetch(s)) for s in sources]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    merged = []

    for src, items in zip(sources, results):
        if not items:
            continue

        for r in items:
            if isinstance(r, dict) and "raw" in r:
                merged.append(r)
            else:
                merged.append({
                    "raw": r,
                    "source": getattr(src, "url", getattr(src, "folder_id", "unknown"))
                })

    # ===============================================
    # DEDUPLICACIÓN
    # ===============================================
    seen = set()
    deduped = []

    for w in merged:
        raw = w.get("raw", {})

        key = str(
            raw.get("dni")
            or raw.get("correo")
            or raw.get("id")
            or raw.get("documento")
            or json.dumps(raw, sort_keys=True)
        )

        if key not in seen:
            seen.add(key)
            deduped.append(w)

    logger.info(f"[merge_sources] fetched {len(merged)} items -> deduped {len(deduped)}")

    return deduped
