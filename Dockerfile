FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
# Render will inject $PORT; use it
CMD ["bash","-lc","uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8899}"]
