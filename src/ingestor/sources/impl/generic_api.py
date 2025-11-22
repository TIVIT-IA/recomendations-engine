# ingestor/src/sources/impl/generic_api.py
import aiohttp
import asyncio
import logging
from typing import List, Dict, Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from ingestor.sources.base_source import BaseSource

logger = logging.getLogger("generic_api")

class GenericAPISource(BaseSource):
    def __init__(self, url, headers=None, params=None, page_param="page", per_page_param="per_page", page_size=100, max_pages=10, timeout=10):
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
        self.page_param = page_param
        self.per_page_param = per_page_param
        self.page_size = page_size
        self.max_pages = max_pages
        self.timeout = timeout

    async def _fetch_page(self, session, url, params):
        async with session.get(url, headers=self.headers, params=params, timeout=self.timeout) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def fetch(self) -> List[Dict[str,Any]]:
        """
        Retorna lista de dicts normalizados desde cualquier API GET.
        Intenta paginar si la API lo permite.
        """
        results = []
        async with aiohttp.ClientSession() as session:
            # primera llamada
            params = dict(self.params)
            params[self.per_page_param] = self.page_size
            params[self.page_param] = 1

            for p in range(1, self.max_pages + 1):
                params[self.page_param] = p
                try:
                    data = await self._fetch_page(session, self.url, params)
                except Exception as e:
                    logger.warning(f"[generic_api] error fetching page {p} from {self.url}: {e}")
                    break

                # normalizar estructura
                if isinstance(data, list):
                    page_items = data
                elif isinstance(data, dict):
                    page_items = data.get("items") or data.get("data") or data.get("results") or data.get("records") or []
                else:
                    page_items = []

                if not page_items:
                    break

                for r in page_items:
                    results.append({"raw": r, "source": self.url})

                # heurística: si less than page_size, fin
                if len(page_items) < self.page_size:
                    break

        return results
