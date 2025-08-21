from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import RedirectResponse
import os

app = FastAPI()

# Админ/UI
app.mount("/ui", StaticFiles(directory="app/ui", html=True), name="ui")

# Если есть старый дашборд в app/static/index.html — делаем его главной /
if os.path.exists("app/static/index.html"):
    app.mount("/", StaticFiles(directory="app/static", html=True), name="root_static")
else:
    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse(url="/ui/")
