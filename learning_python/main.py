import queue
import click
import signal
import asyncio
import logging

from typing import Set

from learning_python.storage import Storage
from learning_python.workers import crawling
from learning_python.workers import downloading

from learning_python.workers import CrawlRequest
from learning_python.workers import CrawlResponse
from learning_python.workers import DownloadRequest
from learning_python.workers import DownloadResponse

from concurrent.futures import ThreadPoolExecutor


def coordinate(incoming: queue.Queue, crawl_requests: queue.Queue, download_requests: queue.Queue):
    logger = logging.getLogger("coordinator")
    storage = Storage.open("/unsplash")
    queued: Set[str] = set()

    while batch := storage.pop_batch():
        logger.info(f"Looking for items to download ...")
        for id, url in storage.get_downloadable():
            if id not in queued:
                queued.add(id)
                download_requests.put(DownloadRequest(id=id, url=url))

        logger.info(f"Looking for items to follow ...")
        for id, follow in batch.items():
            crawl_requests.put(CrawlRequest(id=id, follow=follow))

        try:
            logger.info(f"Looking for responses ...")
            while value := incoming.get(block=True, timeout=1):
                if isinstance(value, CrawlResponse):
                    next = {id: follow for id, _, _, follow in value.data if follow and not storage.is_visited(id)}

                    for id, url, _, _ in value.data:
                        if not storage.is_visited(id):
                            logger.debug(f"Visiting {id} ...")
                            storage.set_visited(id, url)

                    if next:
                        storage.push_batch(next)

                if isinstance(value, DownloadResponse):
                    queued.remove(value.id)
                    data = value.data
                    if data:
                        logger.debug(f"Downloaded {value.id} / {len(data)}")
                        storage.set_downloaded(value.id, data)

            return

        except queue.Empty:
            logger.info(f"No more responses, continuing ...")
            logger.info(f"Visited: {storage.visited_count()}")
            logger.info(f"Downloaded: {storage.downloaded_count()}")


@click.command()
def unsplash() -> None:
    incoming = queue.Queue()
    crawl_requests = queue.Queue(maxsize=30)
    download_requests = queue.Queue()

    logger = logging.getLogger("main")
    crawlers, downloaders = 6, 6

    async def execute():
        loop = asyncio.get_running_loop()

        with ThreadPoolExecutor(max_workers=20) as executor:
            crawl_tasks = [
                loop.run_in_executor(executor, crawling, crawl_requests, incoming, f"/tmp/profile-{i}")
                for i in range(crawlers)
            ]

            download_tasks = [
                loop.run_in_executor(executor, downloading, download_requests, incoming) for _ in range(downloaders)
            ]

            coordinate_task = loop.run_in_executor(executor, coordinate, incoming, crawl_requests, download_requests)

            await asyncio.wait([*crawl_tasks, *download_tasks, coordinate_task])

    def terminate_ignore(signum, frame):
        logger.warn("Handling CTRL+C, waiting for workers...")

    def terminate_everything(signum, frame):
        logger.warn("Handling CTRL+C, waiting for workers...")

        for _ in range(downloaders):
            download_requests.put(None)

        for _ in range(crawlers):
            crawl_requests.put(None)

        incoming.put(None)
        signal.signal(signal.SIGINT, terminate_ignore)

    format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=format)


    signal.signal(signal.SIGINT, terminate_everything)
    asyncio.run(execute())
