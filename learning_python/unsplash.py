import re

from typing import List
from typing import Iterator
from typing import Optional

from dataclasses import dataclass

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement


@dataclass(kw_only=True)
class ImageAttribution:
    id: Optional[str]
    url: Optional[str]
    code: Optional[str]
    follow: Optional[str]
    tags: Optional[List[str]]
    description: Optional[str]

    @staticmethod
    def empty() -> "ImageAttribution":
        return ImageAttribution(id=None, url=None, code=None, follow=None, tags=None, description=None)


def _extract_id(element: WebElement) -> ImageAttribution:
    href = element.get_attribute("href")
    pattern = r"/photos/(?P<id>[^/]{11})$"

    if found := re.search(pattern, href):
        return ImageAttribution(
            id=None,
            url=None,
            tags=None,
            follow=href,
            code=found.group("id"),
            description=element.get_attribute("title"),
        )

    return ImageAttribution.empty()


def _extract_tags(driver: webdriver.Chrome) -> List[str]:
    xpath1 = "//*[contains(text(), 'Related tags')]/following-sibling::div[1]/div/a[@title and @href]"
    xpath2 = "//h3[contains(text(), 'Views')]/../../../following-sibling::div/a[@title and @href]"

    data1 = [e.text for e in driver.find_elements(By.XPATH, xpath1)]
    data2 = [e.text for e in driver.find_elements(By.XPATH, xpath2)]

    return data1 if data1 else data2


def _extract_first_width(driver: webdriver.Chrome, element: WebElement) -> ImageAttribution:
    srcset = element.get_attribute("srcset")
    pattern = r"^(?P<url>https://images.unsplash.com/photo-(?P<id>[0-9]{13}-[0-9a-f]{12})\\?[^\s]+)\s+[0-9]{3,4}w"

    links = element.find_elements(By.XPATH, ".//ancestor::a[1]")
    attribution = _extract_id(links[0]) if links else ImageAttribution.empty()

    if urls := re.search(pattern, srcset):
        attribution.id = urls.group("id")
        attribution.url = urls.group("url")

    if description := element.get_attribute("alt"):
        attribution.description = description

    if not links and not element.get_attribute("itemprop") and not attribution.follow:
        if found := re.search(r"/photos/(?P<id>[^/]{11})$", driver.current_url):
            attribution.follow = driver.current_url
            attribution.code = found.group("id")
            attribution.tags = _extract_tags(driver)

    return attribution


def extract_visible_images(driver: webdriver.Chrome) -> Iterator[ImageAttribution]:
    for item in driver.find_elements(By.XPATH, "//img[@srcset]"):
        if attribution := _extract_first_width(driver, item):
            if attribution.id and attribution.url:
                yield attribution
