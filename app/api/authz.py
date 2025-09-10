# app/api/authz.py
from fastapi import APIRouter
import os


router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/meta")
def meta():
return {
"supabase_url": os.getenv("SUPABASE_URL"),
"supabase_anon_key": os.getenv("SUPABASE_ANON_KEY"),
}


@router.get("/ping")
def ping():
return {"ok": True}
