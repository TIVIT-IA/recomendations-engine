import aiohttp
import logging
from typing import List, Dict, Any
from ingestor.sources.base_source import BaseSource

logger = logging.getLogger("generic_api")


class GenericAPISource(BaseSource):
    """
    Versión corregida:
    - NO agrega page, per_page, ni ningún parámetro adicional.
    - Usa la URL EXACTA definida en el .env.
    - Hace una sola llamada (Supabase entrega todo en una sola respuesta).
    """

    def __init__(self, url, headers=None, params=None, timeout=20):
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
        self.timeout = timeout

    async def fetch(self) -> List[Dict[str, Any]]:
        logger.info(f"[generic_api] usando headers: {self.headers}")

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    self.url,
                    headers=self.headers,
                    params=self.params,  # se respetan los params si existen, NO se agregan nuevos
                    timeout=self.timeout,
                ) as resp:

                    resp.raise_for_status()
                    data = await resp.json()

            except Exception as e:
                logger.warning(f"[generic_api] error fetching from {self.url}: {e}")
                return []

        # Normalización
        if isinstance(data, list):
            return [{"raw": r, "source": self.url} for r in data]

        if isinstance(data, dict):
            items = (
                data.get("items")
                or data.get("data")
                or data.get("results")
                or data.get("records")
                or []
            )
            return [{"raw": r, "source": self.url} for r in items]

        return []
