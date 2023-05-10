import queue
import logging

from typing import Set
from typing import Tuple

from learning_python.storage import Storage
from learning_python.workers import CrawlRequest
from learning_python.workers import CrawlResponse
from learning_python.workers import DownloadRequest
from learning_python.workers import DownloadResponse


def handle_responses(
    storage: Storage,
    logger: logging.Logger,
    incoming: queue.Queue,
    queued: Set[str],
    widths: Tuple[int],
    timeout: float = 1,
) -> bool:
    considered, taken, downloaded, bytes = 0, 0, 0, 0
    logger.info(f"Looking for responses ...")

    try:
        while value := incoming.get(block=True, timeout=timeout):
            if isinstance(value, CrawlResponse):
                next = {id: follow for id, _, _, follow in value.data if follow and not storage.is_visited(id)}

                for id, url, _, _ in value.data:
                    considered = considered + 1
                    if not storage.is_visited(id):
                        logger.debug(f"Visiting {id} ...")
                        storage.set_visited(id, url)
                        taken = taken + 1

                if next:
                    storage.push_batch(next)

            if isinstance(value, DownloadResponse):
                queued.remove(value.id)
                data = value.data
                if data:
                    logger.debug(f"Downloaded {value.id} / {len(data)}")
                    storage.set_downloaded(value.width, value.id, data)
                    downloaded = downloaded + 1
                    bytes = bytes + len(data)

        return False

    except queue.Empty:
        logger.info(f"No more responses, continuing ...")
        logger.info(f"Processed: {taken} / {considered} + {downloaded} / {bytes}")
        logger.info(f"Visited: {storage.visited_count()}")

        for width in widths:
            left = storage.visited_count() - storage.downloaded_count(width)
            logger.info(f"Downloaded: {storage.downloaded_count(width)} + {left} / {width}")

        return True


def coordinate(
    incoming: queue.Queue,
    crawl_requests: queue.Queue,
    crawl_limit: int,
    download_requests: queue.Queue,
    download_widths: Tuple[int],
    download_batch_size: int,
) -> None:
    logger = logging.getLogger("coordinator")
    storage = Storage.open("/unsplash", widths=download_widths)
    queued: Set[str] = set()

    logger.info(f"Bootstrapping queue ...")
    crawl_requests.put(CrawlRequest(id=None, follow="https://unsplash.com/"))
    continuable = handle_responses(storage, logger, incoming, queued, download_widths, timeout=20)
    
    if not continuable:
        return

    while True:
        for width in download_widths:
            downloadable = 0
            logger.info(f"Looking for items to download {width} ...")

            for id, url in storage.get_downloadable(download_batch_size, width, queued):
                if id not in queued:
                    queued.add(id)
                    downloadable = downloadable + 1
                    download_requests.put(DownloadRequest(id=id, url=url, width=width))

            logger.info(f"Looking for items to download {width}: {downloadable}")

        if crawl_limit > storage.visited_count():
            if batch := storage.pop_batch():
                followable = 0
                logger.info(f"Looking for items to follow ...")

                for id, follow in batch.items():
                    followable = followable + 1
                    crawl_requests.put(CrawlRequest(id=id, follow=follow))

                logger.info(f"Looking for items to follow: {followable}")

        if not handle_responses(storage, logger, incoming, queued, download_widths):
            return