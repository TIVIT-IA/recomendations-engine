# ingestor/src/sources/impl/drive_source.py
import io
import os
import asyncio
import logging
from typing import List, Dict, Any
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
import docx
import PyPDF2
import aiofiles
import hashlib
import json

from ingestor.sources.base_source import BaseSource
import aioredis

logger = logging.getLogger("drive_source")

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def sha256_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()

class DriveSource(BaseSource):
    def __init__(self, folder_id: str, service_account_file: str = None, page_size: int = 100):
        self.folder_id = folder_id
        self.service_account_file = service_account_file or os.getenv("SERVICE_ACCOUNT_FILE")
        self.page_size = page_size
        self.redis_url = os.getenv("REDIS_URL", None)  # opcional

    def _build_service(self):
        creds = Credentials.from_service_account_file(self.service_account_file, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        return service

    def _download_file(self, service, file_id: str) -> bytes:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()

    def _extract_text_from_bytes(self, content_bytes: bytes, mime_type: str) -> str:
        if mime_type == "application/pdf":
            reader = PyPDF2.PdfReader(io.BytesIO(content_bytes))
            text = []
            for p in reader.pages:
                page_text = p.extract_text()
                if page_text:
                    text.append(page_text)
            return "\n".join(text[:1000])  # limita texto muy grande
        elif mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            fh = io.BytesIO(content_bytes)
            doc = docx.Document(fh)
            return "\n".join([p.text for p in doc.paragraphs])
        else:
            try:
                return content_bytes.decode('utf-8', errors='ignore')
            except Exception:
                return ""

    async def fetch(self) -> List[Dict[str, Any]]:
        """Lista archivos de la carpeta y extrae texto. Evita redescargar archivos no cambiados."""
        loop = asyncio.get_event_loop()
        service = await loop.run_in_executor(None, self._build_service)

        # list files
        query = f"'{self.folder_id}' in parents and trashed = false"
        files = []
        page_token = None
        while True:
            resp = service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType, md5Checksum, size, modifiedTime)", pageSize=self.page_size, pageToken=page_token).execute()
            files.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        # connect redis if configured
        redis = None
        if self.redis_url:
            redis = await aioredis.from_url(self.redis_url, decode_responses=True)

        results = []
        for f in files:
            file_id = f["id"]
            name = f.get("name")
            mime_type = f.get("mimeType", "")
            md5 = f.get("md5Checksum") or f.get("modifiedTime") or str(f.get("size", "0"))

            cache_key = f"drive:file:{file_id}:md5"
            # check redis for same md5
            if redis:
                old = await redis.get(cache_key)
                if old == md5:
                    logger.info(f"[drive] skip unchanged {name}")
                    continue

            # download (blocking in executor)
            content_bytes = await loop.run_in_executor(None, self._download_file, service, file_id)
            text = await loop.run_in_executor(None, self._extract_text_from_bytes, content_bytes, mime_type)

            # store md5 in redis
            if redis:
                await redis.set(cache_key, md5, ex=60 * 60 * 24 * 7)  # 7 days

            results.append({
                "raw": {"documento": name, "contenido": text, "mime_type": mime_type, "file_id": file_id},
                "source": "google_drive"
            })

        return results
