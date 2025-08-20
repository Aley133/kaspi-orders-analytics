$env:PYTHONUTF8=1
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
$env:PORT = if ($env:PORT) { $env:PORT } else { "8899" }
.\.venv\Scripts\python -m uvicorn app.main:app --host 127.0.0.1 --port $env:PORT --log-level debug
