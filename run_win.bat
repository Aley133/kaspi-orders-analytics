@echo off
SET PORT=8899
python -m venv .venv
call .venv\Scripts\activate
pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port %PORT% --log-level debug
