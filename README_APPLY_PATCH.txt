Патч v0.6.1 (Profit + Inventory FIFO + Catalog):

1) Распакуйте архив в корень проекта (поверх файлов), ZIP в репозитории не нужен.
2) Откройте app/api/routes.py и вставьте блоки из app/api/routes_patch.txt:
   - импорты
   - PROFIT endpoints (/profit/*)
   - INVENTORY endpoints (/inventory/*)
   - CATALOG endpoints (/catalog/*)
3) Коммит → пуш в GitHub → Render: Manual Deploy → Clear build cache & deploy.
4) Страницы:
   - /ui/profit.html — Профит (обороты/комиссии/чистая прибыль + ручная себестоимость)
   - /ui/inventory.html — Склад (приход, пороги, FIFO)
   - /ui/catalog.html — Каталог (⇅ Выгрузить товары из «Управление товарами», сводка активен/неактивен/остаток)

Данные (себестоимость/склад/каталог) хранятся в SQLite app/data/app.db.
На Render они не переживут redeploy без persistent disk — подключите диск или вынесите в Postgres/Supabase.
