# Kaspi Orders Analytics — Refactor (Cut-off mode, Themes, Order IDs CSV)

## Что нового
- **Режим cut-off** (сдвиг суток): `plannedShipmentDate` ≤ `DAY_CUTOFF` с хвостами через `PACK_LOOKBACK_DAYS`
- **Пресет "Kaspi: Упаковка (до cut-off)"** в UI
- **Светлая/тёмная тема** (кнопка "Тема", хранится в `localStorage`)
- **"Номера заказов (для сверки)"** — список, копирование, CSV
- Модульная структура: `api / core / services / ui`

## Быстрый старт
```bash
python -m venv .venv
source .venv/bin/activate # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env  # заполните KASPI_TOKEN и при необходимости TZ/DAY_CUTOFF
uvicorn app.main:app --host 0.0.0.0 --port 8899
# UI: http://localhost:8899/ui/
```

## .env
```
KASPI_TOKEN=...
TZ=Asia/Almaty
DAY_CUTOFF=20:00
PACK_LOOKBACK_DAYS=3
AMOUNT_FIELDS=totalPrice
AMOUNT_DIVISOR=1
CHUNK_DAYS=7
DATE_FIELD_DEFAULT=creationDate
DATE_FIELD_OPTIONS=creationDate,plannedShipmentDate,plannedDeliveryDate,shipmentDate,deliveryDate
HOST=0.0.0.0
PORT=8899
DEBUG=true
```

## Presets
- **Пришло сегодня**: `creationDate`, [сегодня…сегодня]
- **План на сегодня**: `plannedDeliveryDate`, ≤ cut-off
- **Доставлено**: `deliveryDate`, state=`COMPLETED`
- **Kaspi: Упаковка (до cut-off)**: `plannedShipmentDate`, `use_cutoff_window=true`, `lte_cutoff_only=true`, states в работе (`NEW,ACCEPTED_BY_MERCHANT,DELIVERY`), исключены `COMPLETED,CANCELLED,DELIVERY_TRANSFERRED,RETURNED`

> Примечание по API Kaspi: параметры фильтра по датам могут отличаться в разных аккаунтах/версиях.
> В `app/core/kaspi.py` используется вид `filter[{date_field}][ge/le]` и пагинация `page[number]/page[size]`.
> Если ваш контракт другой — адаптируйте ключи в одном месте.
