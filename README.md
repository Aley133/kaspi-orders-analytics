# Kaspi Orders Daily Count Service (LeoXpress, v0.2)

Готовый к запуску сервис на **FastAPI** для магазина **LeoXpress** (Partner ID: 30295031).
Он напрямую обращается к API Kaspi и выдаёт количество заказов по дням за указанный период.

## Установка и запуск

### 1) Локально (Windows/Linux/Mac)
```bash
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8899
```
Открой: `http://localhost:8899`

### 2) Docker
```bash
docker build -t kaspi-orders:0.2 .
docker run --rm -p 8899:8899 --env-file .env kaspi-orders:0.2
```

### 3) Быстрые скрипты
- Windows PowerShell: `./run_win.ps1` (если ругается политика – запусти PowerShell «Запуск от имени администратора» и `Set-ExecutionPolicy Bypass -Scope Process`)
- Linux/Mac: `./run_unix.sh`

## Проверка
- `GET /health` — простой ping сервиса.
- `GET /diagnostics/ping-kaspi` — реальный запрос в Kaspi (1 страница, последние 30 дней).
- `GET /orders/daily-count?start=YYYY-MM-DD&end=YYYY-MM-DD[&state=NEW][&tz=Asia/Almaty]` — данные по дням.

Примеры:
```bash
curl "http://localhost:8899/health"
curl "http://localhost:8899/diagnostics/ping-kaspi"
curl "http://localhost:8899/orders/daily-count?start=2025-08-01&end=2025-08-19"
curl "http://localhost:8899/orders/daily-count?start=2025-08-01&end=2025-08-19&state=NEW"
```

## Интерфейс
Главная страница `http://localhost:8899/` — простая форма: выбираешь период и, при желании, state. Результат отображается в таблице сразу из браузера.

## Переменные окружения
- `KASPI_TOKEN` — токен магазина (уже проставлен).
- `PORT` — порт (по умолчанию 8899).
- `TZ` — таймзона (по умолчанию Asia/Almaty).
- `DEFAULT_STATES` — список разрешённых состояний, например `NEW,PICKUP`.
- `CACHE_TTL` — TTL кэша (сек).
- `PARTNER_ID`, `SHOP_NAME` — метаданные (для `/meta`).

## Примечания
- Пагинация: до 100 на страницу, сервис пройдёт все страницы.
- Ретраи: на 429/5xx и сетевые ошибки.
- Таймзона: агрегация по дням в заданной таймзоне.
