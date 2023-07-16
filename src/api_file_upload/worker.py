from typing import Any

import aiohttp

from api_file_upload.job import UploadFromURLJob

# Reference: https://arq-docs.helpmanual.io/#simple-usage

DOWNLOAD_CHUNK_SIZE = 1 * 1024 * 1024


async def process_job(ctx: dict[str, Any], job: UploadFromURLJob) -> None:
    print(f"Downloading file from '{job.url}'...")
    session: aiohttp.ClientSession = ctx["session"]
    async with session.get(job.url) as res:
        if res.status >= 400:
            print(f"Failed to download file from {job.url}")
        file_size = 0
        async for chunk in res.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
            file_size += len(chunk)
    print(f"File size: {(file_size/1024/1024):.2f} MiB")


async def startup(ctx):
    ctx["session"] = aiohttp.ClientSession()


async def shutdown(ctx):
    await ctx["session"].close()


class WorkerSettings:
    functions = [process_job]
    on_startup = startup
    on_shutdown = shutdown
