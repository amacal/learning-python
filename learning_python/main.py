import os
import queue
import click
import signal
import asyncio
import logging

from typing import Any
from typing import List
from typing import Tuple
from typing import Callable
from typing import NamedTuple

from learning_python.workers import crawling
from learning_python.workers import downloading
from learning_python.coordinator import coordinate

from concurrent.futures import ThreadPoolExecutor


def get_signal_handler(logger: logging.Logger, queues: List[queue.Queue]) -> Callable[[int, Any], None]:
    def terminate_ignore(signum: int, frame: Any) -> None:
        logger.warn(f"Handling CTRL+C, waiting for workers ({os.getpid()}) ...")

    def terminate_everything(signum: int, frame: Any) -> None:
        logger.warn(f"Handling CTRL+C, waiting for workers ({os.getpid()}) ...")

        for queue in queues:
            queue.put(None)

        signal.signal(signal.SIGINT, terminate_ignore)

    return terminate_everything


@click.command()
@click.option("--crawlers", default=6, type=int)
@click.option("--downloaders", default=6, type=int)
@click.option("--download-batch-size", default=50, type=int)
@click.option("--width", default=[600], type=int, multiple=True)
@click.option("--crawl-limit", default=(1000000), type=int)
def unsplash(
    crawlers: int,
    downloaders: int,
    download_batch_size: int,
    width: Tuple[int],
    crawl_limit: int,
) -> None:
    incoming: queue.Queue = queue.Queue()
    crawl_requests: queue.Queue = queue.Queue(maxsize=30)
    download_requests: queue.Queue = queue.Queue()
    logger = logging.getLogger("main")

    async def execute():
        loop = asyncio.get_running_loop()

        class CrawlingArgs(NamedTuple):
            incoming: queue.Queue
            outgoing: queue.Queue
            profile: str

        def create_crawl_args(index: int) -> CrawlingArgs:
            return CrawlingArgs(
                incoming=crawl_requests,
                outgoing=incoming,
                profile=f"/tmp/profile-{index}",
            )

        class CoordinateArgs(NamedTuple):
            incoming: queue.Queue
            crawl_requests: queue.Queue
            crawl_limit: int
            download_workers: int
            download_requests: queue.Queue
            download_widths: Tuple[int]
            download_batch_size: int

        def create_coordinate_args() -> CoordinateArgs:
            return CoordinateArgs(
                incoming=incoming,
                crawl_requests=crawl_requests,
                crawl_limit=crawl_limit,
                download_workers=downloaders,
                download_requests=download_requests,
                download_widths=width,
                download_batch_size=download_batch_size,
            )

        with ThreadPoolExecutor(max_workers=1 + downloaders + crawlers) as executor:
            crawl_args = [create_crawl_args(i) for i in range(crawlers)]
            crawl_tasks = [loop.run_in_executor(executor, crawling, *args) for args in crawl_args]

            download_args = [(download_requests, incoming) for _ in range(downloaders)]
            download_tasks = [loop.run_in_executor(executor, downloading, *args) for args in download_args]

            coordinate_args = [create_coordinate_args() for _ in range(1)]
            coordinate_task = [loop.run_in_executor(executor, coordinate, *args) for args in coordinate_args]

            await asyncio.wait([*crawl_tasks, *download_tasks, *coordinate_task])

    format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=format)

    queues = [incoming] + [crawl_requests] * crawlers + [download_requests] * downloaders
    signal.signal(signal.SIGINT, get_signal_handler(logger, queues))

    asyncio.run(execute())
