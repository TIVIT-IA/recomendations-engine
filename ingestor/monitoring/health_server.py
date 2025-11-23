# ingestor/src/health_server.py
from aiohttp import web
from ingestor.health import create_app

if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=int(__import__("os").environ.get("INGEST_HEALTH_PORT", "9001")))
