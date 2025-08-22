from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from .api.routes import api_router
from .core.config import settings

app = FastAPI(title="Kaspi Orders Analytics", docs_url="/docs", redoc_url="/redoc")

# CORS (open for local; lock down in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API
app.include_router(api_router, prefix="/api")

# Static UI
app.mount("/ui", StaticFiles(directory=str((__file__[:-8]) + "ui"), html=True), name="ui")

# Root redirect -> UI
@app.get("/")
def root():
    return {"ok": True, "ui": "/ui/"}
