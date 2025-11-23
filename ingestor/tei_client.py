
import aiohttp
import asyncio
import backoff
import logging

class TEIClient:
    def __init__(self, base_url: str, max_batch: int = 32, timeout: int = 60):
        self.base_url = base_url.rstrip('/')
        self.max_batch = max_batch
        self.timeout = timeout
        self.logger = logging.getLogger("TEIClient")

    async def _post(self, session, texts):
        url = f"{self.base_url}/embed"
        payload = {"inputs": texts}

        async with session.post(url, json=payload, timeout=self.timeout) as resp:
            resp.raise_for_status()
            data = await resp.json()

            # 1️⃣ TEI tradicional: devuelve directamente una lista de vectores
            if isinstance(data, list):
                return data

            # 2️⃣ Formato {"embeddings": [...]}
            if isinstance(data, dict) and "embeddings" in data:
                return data["embeddings"]

            # 3️⃣ Formato {"data": [{embedding: [...]}, ...]}
            if isinstance(data, dict) and "data" in data:
                return [d.get("embedding") for d in data["data"]]

            # 4️⃣ Cualquier otra cosa es error
            self.logger.error(f"TEI devolvió formato inesperado: {data}")
            raise RuntimeError("Formato TEI inesperado sin embeddings")

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_time=60)
    async def embed_batch(self, session: aiohttp.ClientSession, texts: list):
        all_embeddings = []

        for i in range(0, len(texts), self.max_batch):
            chunk = texts[i:i + self.max_batch]
            self.logger.info(f"→ TEI embedding batch {i}-{i+len(chunk)}")
            emb = await self._post(session, chunk)
            all_embeddings.extend(emb)

        return all_embeddings
