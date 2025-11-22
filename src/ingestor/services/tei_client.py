# ingestor/src/clients/tei_client.py
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

            # TEI returns: { "embeddings": [...] }
            if "embeddings" in data:
                return data["embeddings"]

            # HF Inference API format
            if "data" in data:
                return [d.get("embedding") for d in data["data"]]

            raise RuntimeError(f"Formato TEI inesperado: {data}")

    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_time=60)
    async def embed_batch(self, session: aiohttp.ClientSession, texts: list):
        """
        Divide batch grande en mini-lotes porque los modelos TEI
        no aceptan batches enormes.
        """
        all_embeddings = []

        # dividir batch
        for i in range(0, len(texts), self.max_batch):
            chunk = texts[i:i + self.max_batch]

            self.logger.info(f"→ TEI embedding chunk {i}-{i+len(chunk)}")

            emb = await self._post(session, chunk)
            all_embeddings.extend(emb)

        return all_embeddings
