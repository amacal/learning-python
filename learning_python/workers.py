import queue
import logging
import requests

from typing import List
from typing import Tuple
from typing import Optional

from dataclasses import dataclass
from learning_python.unsplash import extract_visible_images

from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait


@dataclass(kw_only=True)
class DownloadRequest:
    id: str
    url: str
    width: int


@dataclass(kw_only=True)
class DownloadResponse:
    id: str
    width: int
    data: Optional[bytes]


def downloading(incoming: queue.Queue, outgoing: queue.Queue):
    logger = logging.getLogger("downloading")

    value: Optional[DownloadRequest]
    while value := incoming.get():
        try:
            logger.debug(f"Downloading {value.id} ...")
            response = requests.get(value.url)
            
            response.raise_for_status()
            outgoing.put(DownloadResponse(id=value.id, width=value.width, data=response.content))
        except Exception as ex:
            outgoing.put(DownloadResponse(id=value.id, width=value.width, data=None))
            logger.error(str(ex))


@dataclass(kw_only=True)
class CrawlRequest:
    id: Optional[str]
    follow: str


@dataclass(kw_only=True)
class CrawlResponse:
    data: List[Tuple[str, str, Optional[str], Optional[str]]]


def wait_for_network_idle(driver: Chrome, idle_threshod: float = 5, timeout: float = 30) -> bool:
    def _get_network_idle_seconds() -> str:
        return f"""
            const now = performance.now();
            const latest = window
                .performance.getEntries()
                .filter(x => x.responseEnd)
                .reduce((latest, e) => Math.max(latest, e.responseEnd), 0);

            return (now - latest) / 1000;
        """

    def predicate(driver: Chrome) -> bool:
        idle_script: str = _get_network_idle_seconds()
        idle_seconds: float = driver.execute_script(idle_script)

        return idle_seconds >= idle_threshod

    return WebDriverWait(driver, timeout).until(predicate)


def crawling(incoming: queue.Queue, outgoing: queue.Queue, profile: str):
    options = Options()
    options.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })

    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-data-dir={profile}")

    driver = Chrome(options=options)
    driver.set_window_size(1920, 1080)
    logger = logging.getLogger("crawling")

    value: Optional[CrawlRequest]
    while value := incoming.get():
        try:
            driver.get(value.follow)
            logger.debug(f"Following {value.id} / {value.follow} ...")

            wait_for_network_idle(driver, idle_threshod=1)
            data = list(extract_visible_images(driver))
            outgoing.put(CrawlResponse(data=data))
        except Exception as ex:
            logger.error(str(ex))
