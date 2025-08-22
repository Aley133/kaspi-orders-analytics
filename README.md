# Kaspi Orders Analytics — Fix (no-reload buttons)

Что изменено:
- Все кнопки в UI имеют `type="button"` — страница больше не перезагружается.
- JS глушит любые submit'ы и вызывает `preventDefault()` на кликах.
- Ссылка «Скачать CSV» не активна до первого запроса, потом включается.
- Надёжный редирект `/` → `/ui/`, корректное монтирование статики через `pathlib`.
- Добавлен эндпоинт диагностики `/api/diagnostics/ping-kaspi`.

## Запуск
```bash
pip install -r requirements.txt
cp .env.example .env  # укажи KASPI_TOKEN
uvicorn app.main:app --host 0.0.0.0 --port 8899
# UI: http://localhost:8899/ui/
```

## Render (Start Command)
```
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```
