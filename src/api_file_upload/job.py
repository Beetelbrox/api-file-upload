from dataclasses import dataclass

from arq import ArqRedis


@dataclass(frozen=True)
class UploadFromURLJob:
    url: str


class JobScheduler:
    def __init__(self, pool: ArqRedis) -> None:
        self._pool = pool

    async def submit_job(self, job: UploadFromURLJob) -> None:
        await self._pool.enqueue_job("process_job", job)
