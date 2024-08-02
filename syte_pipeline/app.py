import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uptrace
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette import status
from starlette.responses import JSONResponse


import syte_pipeline
from syte_pipeline.examples import v0_router
from syte_pipeline.s1.analytic import v1
from syte_pipeline.settings import Settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator:
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    logger.info(
        "Application started. You can check the documentation \
        in https://localhost:8000/docs/"
    )
    yield
    # Shut Down
    logger.warning("Application shutdown")


app = FastAPI(
    title=syte_pipeline.__name__,
    version=syte_pipeline.__version__,
)

uptrace.configure_opentelemetry(
    # Copy DSN here or use UPTRACE_DSN env var.
    dsn=Settings().telemetry_dsn,
    service_name=syte_pipeline.__name__,
    service_version=syte_pipeline.__version__,
    logging_level=logging.INFO,
)
FastAPIInstrumentor.instrument_app(app)
app.include_router(v0_router)
app.include_router(v1)


@app.get("/health", status_code=200)
async def get_health() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content="ok",
    )


@app.get("/version", status_code=200)
async def get_version() -> dict:
    return {"version": syte_pipeline.__version__}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000, access_log=False)


if __name__ == "__main__":
    main()
