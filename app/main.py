# app/main.py
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from .api.routes import api_router
from .core.config import settings

app = FastAPI(title="Kaspi Orders Analytics", docs_url="/docs", redoc_url="/redoc")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API
app.include_router(api_router, prefix="/api")

# 🔧 НАДЁЖНОЕ монтирование статики
UI_DIR = Path(__file__).resolve().parent / "ui"
app.mount("/ui", StaticFiles(directory=UI_DIR, html=True), name="ui")

# 🔁 Редирект с корня на /ui/
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/ui/")
