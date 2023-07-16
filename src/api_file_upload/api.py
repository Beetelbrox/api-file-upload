import functools
import os
import uuid
from typing import Annotated, Callable, Optional, Tuple, TypeVar

from anyio import Semaphore
from arq import ArqRedis, create_pool
from arq.connections import RedisSettings
from fastapi import Depends, FastAPI, Request
from starlette.concurrency import run_in_threadpool
from streaming_form_data import StreamingFormDataParser
from streaming_form_data.targets import S3Target

from api_file_upload.job import JobScheduler, UploadFromURLJob
from api_file_upload.services import create_signed_url, refresh_token

T = TypeVar("T")

# https://stackoverflow.com/questions/73442335/how-to-upload-a-large-file-%E2%89%A53gb-to-fastapi-backend
# Apparently spawning way too many threads is bad for starlette's health, explained here:
# https://anyio.readthedocs.io/en/stable/threads.html
MAX_CONCURRENT_THREADS = 10
MAX_THREADS_GUARD = Semaphore(MAX_CONCURRENT_THREADS)

# Ugly but effective way to blow up loudly if the configs are not set
SERVICE_ACCOUNT_EMAIL = os.environ["FILE_UPLOAD_SERVICE_ACCOUNT_EMAIL"]
STAGING_BUCKET = os.environ["FILE_UPLOAD_STAGING_BUCKET"]

app = FastAPI()

# Services
pool: Optional[ArqRedis] = None


# FastAPI is not great at sharing connections over requests, and Aiohttp (arq) is quite picky about
# Where the connections are created so this is the less verbose solution I found for this quick and dirty example
# For more info, see: https://github.com/tiangolo/fastapi/discussions/9097.
async def get_scheduler() -> JobScheduler:
    global pool  # Forgive me father for I have sinned
    if pool is None:
        pool = await create_pool(RedisSettings())
    return JobScheduler(pool)


@app.post("/upload")
async def upload_file(request: Request, scheduler: Annotated[JobScheduler, Depends(get_scheduler)]):
    print("Request received")
    filename = f"{uuid.uuid4()}.ndjson"
    file_location = f"gs://{STAGING_BUCKET}/{filename}"
    parser, target = configure_parser(request, file_location)

    print("Refreshing token...")
    # This takes ~0.5 s from a home internet. In a real context
    # We might want to cache the token and only refresh it when it's needed
    token = await run_in_threadpool_guarded(refresh_token)()

    print("Uploading file to cloud storage...")
    async for chunk in request.stream():
        await run_in_threadpool_guarded(parser.data_received)(chunk)

    print("Submitting job...")
    # I know, hacky AF. But didn't want to instantiate more blobs for this example
    url = await run_in_threadpool_guarded(create_signed_url)(target._fd._blob, SERVICE_ACCOUNT_EMAIL, token)
    job = UploadFromURLJob(url)
    await scheduler.submit_job(job)

    return {"message": "File successfully uploaded"}


@app.on_event("shutdown")
async def shutdown():
    global pool
    if pool is not None:
        await pool.close()


def run_in_threadpool_guarded(func: Callable[..., T]):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs) -> T:
        async with MAX_THREADS_GUARD:
            return await run_in_threadpool(func, *args, **kwargs)

    return wrapper


def configure_parser(request: Request, location: str) -> Tuple[StreamingFormDataParser, S3Target]:
    parser = StreamingFormDataParser(headers=request.headers)
    s3_ = S3Target(location, "wb")
    parser.register("file", s3_)
    return parser, s3_
