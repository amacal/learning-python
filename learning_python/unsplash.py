import re

from typing import Tuple
from typing import Iterator
from typing import Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement


def _extract_id(element: WebElement) -> Tuple[Optional[str], Optional[str]]:
    href = element.get_attribute("href")
    pattern = r"/photos/(?P<id>[^/]{11})$"

    if found := re.search(pattern, href):
        return (found.group("id"), href)
    
    return (None, None)


def _extract_600w(element: WebElement) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    srcset = element.get_attribute("srcset")
    pattern = r"(?P<url>https://images.unsplash.com/photo-(?P<id>[0-9]{13}-[0-9a-f]{12})\\?[^\s]+)\s+600w"

    links = element.find_elements(By.XPATH, ".//ancestor::a[1]")
    link: Tuple[Optional[str], Optional[str]] = _extract_id(links[0]) if links else (None, None) 

    urls = re.search(pattern, srcset)
    url: Tuple[Optional[str], Optional[str]] = (urls.group("id"), urls.group("url")) if urls else (None, None) 
    
    return (*url, *link)

            
def extract_visible_images(driver: webdriver.Chrome) -> Iterator[Tuple[str, str, Optional[str], Optional[str]]]:
    for item in driver.find_elements(By.XPATH, "//img[@srcset]"):
        if extracted := _extract_600w(item):
            if len(extracted) == 4 and extracted[0] and extracted[1]:
                yield (extracted[0], extracted[1], extracted[2], extracted[3])
