import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from api.routes import router
from config import GALLERY_DIR, STATIC_PREFIX
from infra.migrations import migrate
from services.metadata_migration import run_startup_migration_if_needed
from services.metadata_store import current_backend, get_store

logger = logging.getLogger(__name__)

app = FastAPI(title="Picture API with Ratings", version="3.0.0")
app.mount(STATIC_PREFIX, StaticFiles(directory=str(GALLERY_DIR), html=False), name="static")
app.include_router(router)

@app.on_event("startup")
def startup():
    migrate()
    get_store()
    logger.info("metadata backend in use: %s", current_backend())
    migration_stats = run_startup_migration_if_needed(current_backend=current_backend())
    if migration_stats is not None:
        logger.info("startup sqlite->pg migration stats: %s", migration_stats)

@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": f"http_{exc.status_code}", "message": "request failed", "detail": exc.detail},
    )

@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"code": "internal_error", "message": "internal server error", "detail": str(exc)},
    )

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


