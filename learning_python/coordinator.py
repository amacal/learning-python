import queue
import logging

from typing import Any
from typing import Set
from typing import Dict
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
    terminated = False
    considered, taken, downloaded, size, orphaned = 0, 0, 0, 0, 0
    logger.info(f"Looking for responses ...")

    try:
        while True:
            value = incoming.get(block=True, timeout=timeout)
            terminated = terminated or value is None

            if isinstance(value, CrawlResponse):
                next = {
                    attribution.id: attribution.follow
                    for attribution in value.data
                    if attribution.id and attribution.follow and not storage.is_visited(attribution.id)
                }

                for attribution in value.data:
                    if attribution.id and attribution.url and attribution.follow:
                        considered = considered + 1
                        logger.debug(f"Visiting {attribution.id} ...")

                        kwargs = {
                            "follow": attribution.follow,
                            "description": attribution.description,
                            "tags": attribution.tags,
                        }

                        def get_flags(payload: Dict[str, Any]) -> str:
                            flags = ""

                            if payload.get("tags"):
                                flags += "t"

                            return flags

                        def merge(previous: Dict[str, Any], next: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
                            payload = {**previous, **next, "tags": next.get("tags", previous.get("tags"))}
                            return (payload, get_flags(payload))

                        visited = storage.set_visited(
                            attribution.id, attribution.url, get_flags(kwargs), kwargs, merge
                        )

                        taken = taken + (1 if visited else 0)

                    else:
                        orphaned += 1

                if next:
                    storage.push_batch(next)

            if isinstance(value, DownloadResponse):
                queued.remove(value.id)
                data = value.data

                if isinstance(data, bytes):
                    logger.debug(f"Downloaded {value.id} / {len(data)}")
                    storage.set_downloaded(value.width, value.id, data)
                    downloaded = downloaded + 1
                    size = size + len(data)

                    if not data:
                        logger.warn(f"Downloaded {value.id} has zero length!")

                else:
                    logger.warn(f"Downloading {value.id} returned no content!")

    except queue.Empty:
        logger.info(f"No more responses, continuing ...")
        logger.info(f"Processed: {taken} / {considered} / {orphaned} + {downloaded} / {size}")
        logger.info(f"Visited: {storage.visited_count()}")

        for version in storage.get_versions():
            logger.info(f"Visited: {storage.visited_count(version)} / {version}")

        for width in widths:
            left = storage.visited_count() - storage.downloaded_count(width)
            logger.info(f"Downloaded: {storage.downloaded_count(width)} + {left} / {width}")

    return terminated is False


def coordinate(
    incoming: queue.Queue,
    crawl_requests: queue.Queue,
    crawl_limit: int,
    download_workers: int,
    download_requests: queue.Queue,
    download_widths: Tuple[int],
    download_batch_size: int,
) -> None:
    logger = logging.getLogger("coordinator")
    queued: Set[str] = set()

    logger.info(f"Initializing storage ...")
    storage = Storage.open("/unsplash", widths=download_widths)

    logger.info(f"Bootstrapping queue ...")
    crawl_requests.put(CrawlRequest(id=None, follow="https://unsplash.com/"))
    continuable = handle_responses(storage, logger, incoming, queued, download_widths, timeout=20)

    if not continuable:
        return

    while True:
        total = 0

        if download_workers:
            for width in download_widths:
                downloadable = 0
                logger.info(f"Looking for items to download {width} ...")

                for id, url in storage.get_downloadable(download_batch_size, width, queued):
                    if id not in queued:
                        queued.add(id)
                        downloadable = downloadable + 1
                        download_requests.put(DownloadRequest(id=id, url=url, width=width))

                total = total + downloadable
                logger.info(f"Looking for items to download {width}: {downloadable}")

        while crawl_limit > storage.visited_count() and crawl_requests.qsize() <= crawl_requests.maxsize * 0.5:
            if batch := storage.pop_batch():
                followable = 0
                logger.info(f"Looking for items to follow ...")

                for id, follow in batch.items():
                    followable = followable + 1
                    crawl_requests.put(CrawlRequest(id=id, follow=follow))

                logger.info(f"Looking for items to follow: {followable}")
            else:
                break

        if not handle_responses(storage, logger, incoming, queued, download_widths):
            return

        total = total + max(0, crawl_limit - storage.visited_count())
        total = total + crawl_requests.qsize() + download_requests.qsize()

        if total == 0:
            for item in [crawl_requests, download_requests]:
                try:
                    for _ in range(max(40, item.maxsize)):
                        item.put_nowait(None)
                except queue.Full:
                    pass

            logger.info("Coordination completed")
            break
