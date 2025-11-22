# ingestor/src/health.py
from aiohttp import web
import asyncio
import os
import json

async def health_handler(request):
    return web.Response(text=json.dumps({"status":"ok"}), content_type="application/json")

def create_app():
    app = web.Application()
    app.router.add_get("/health", health_handler)
    # optionally add /metrics endpoint by importing metrics
    try:
        from ingestor.metrics import metrics_endpoint
        async def metrics_handler(request):
            from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
            return web.Response(body=generate_latest(), content_type=CONTENT_TYPE_LATEST)
        app.router.add_get("/metrics", metrics_handler)
    except Exception:
        pass
    return app

# to run: web.run_app(create_app(), host="0.0.0.0", port=9001)
