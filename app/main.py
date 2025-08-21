from __future__ import annotations
import os
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from .api.routes import router
from .core.config import settings

app = FastAPI(title="Kaspi Orders — Dashboard", version="0.5.0")

app.add_middleware(CORSMiddleware,
                   allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

app.include_router(router, prefix="")

static_dir = os.path.join(os.path.dirname(__file__), "ui")
app.mount("/ui", StaticFiles(directory=static_dir, html=True), name="ui")

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/ui/")
