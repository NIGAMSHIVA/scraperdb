# api/main.py

from fastapi import FastAPI
from api.routes.profiles import router as profiles_router
from api.services.jobs import start_worker

app = FastAPI(title="ScraperDB API", version="0.1")

app.include_router(profiles_router)

@app.on_event("startup")
def _startup():
    #  Starts the in-process background worker thread
    start_worker()
