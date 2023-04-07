from abc import ABC

from typing import List
from typing import Protocol
from typing import Optional

from prompt_toolkit import prompt

from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait

from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException


class ViewTerminal(ABC):
    def prompt_email(self, location: str) -> str:
        return prompt(f"{location}'s email: ")

    def prompt_password(self, location: str) -> str:
        return prompt(f"{location}'s password: ", is_password=True)


class ViewContext(ABC):
    def __init__(self, driver: Chrome, terminal: ViewTerminal) -> None:
        self._driver = driver
        self._terminal = terminal

    @property
    def terminal(self) -> ViewTerminal:
        return self._terminal

    @property
    def driver(self) -> Chrome:
        return self._driver

    def wait_xpath_element(self, xpath: str, timeout: float = 30) -> WebElement:
        xpath_locator = (By.XPATH, xpath)
        xpath_condition = expected_conditions.visibility_of_element_located(xpath_locator)

        return WebDriverWait(self.driver, timeout).until(xpath_condition)

    def wait_for_url_changed(self, url: Optional[str] = None, timeout: float = 30) -> bool:
        url = url or self._driver.current_url
        url_condition = expected_conditions.url_changes(url)

        return WebDriverWait(self._driver, timeout).until(url_condition)

    def wait_for_network_idle(self, idle_threshod: float = 5, timeout: float = 30) -> bool:
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

        return WebDriverWait(self._driver, timeout).until(predicate)


class ViewInterceptor(Protocol):
    def __call__(self) -> bool:
        ...


class ViewProber(Protocol):
    def __call__(self, context: ViewContext) -> Optional[ViewInterceptor]:
        ...


class ViewCallback(Protocol):
    def __call__(self, context: ViewContext) -> bool:
        ...


def safe_probe(func: ViewProber) -> ViewProber:
    def wrapper(context: ViewContext) -> Optional[ViewInterceptor]:
        try:
            return func(context)
        except NoSuchElementException:
            return None

    return wrapper


def safe_intercept(func: ViewInterceptor) -> ViewInterceptor:
    def wrapper() -> bool:
        try:
            return func()
        except NoSuchElementException:
            return False
        except TimeoutException:
            return False

    return wrapper


@safe_probe
def probe_databricks(context: ViewContext) -> Optional[ViewInterceptor]:
    sso_button_xpath = '//button[contains(@class, "sso-btn") and contains(text(), "Single Sign On")]'
    sso_button: WebElement = context.driver.find_element(By.XPATH, sso_button_xpath)

    @safe_intercept
    def intercept() -> bool:
        sso_button.click()
        return True

    return intercept


@safe_probe
def probe_auth0(context: ViewContext) -> Optional[ViewInterceptor]:
    email_input_xpath = '//input[@type="email" and @name="email" and @class="auth0-lock-input"]'
    email_input: WebElement = context.driver.find_element(By.XPATH, email_input_xpath)

    @safe_intercept
    def intercept() -> bool:
        email_input_value: str = context.terminal.prompt_email("Auth0")
        email_input.send_keys(email_input_value)

        login_button_xpath = '//span[contains(@class, "auth0-label-submit")]'
        login_button: WebElement = context.wait_xpath_element(login_button_xpath, timeout=10)

        login_button.click()
        return True

    return intercept


@safe_probe
def probe_microsoft(context: ViewContext) -> Optional[ViewInterceptor]:
    password_input_xpath = '//input[@type="password" and @name="passwd"]'
    password_input: WebElement = context.driver.find_element(By.XPATH, password_input_xpath)

    @safe_intercept
    def intercept() -> bool:
        password_input_value: str = context.terminal.prompt_password("Microsoft")
        password_input.send_keys(password_input_value)

        submit_button_xpath = '//input[@type="submit"]'
        submit_button: WebElement = context.driver.find_element(By.XPATH, submit_button_xpath)

        submit_button.click()
        return True

    return intercept


@safe_probe
def probe_duo(context: ViewContext) -> Optional[ViewInterceptor]:
    duo_iframe_xpath = '//iframe[@id="duo_iframe"]'
    duo_iframe: WebElement = context.driver.find_element(By.XPATH, duo_iframe_xpath)

    @safe_intercept
    def intercept() -> bool:
        context.driver.switch_to.frame(duo_iframe)

        fieldset_xpath = '//fieldset[.//button[contains(text(), "Send Me a Push")]]'
        fieldset: WebElement = context.driver.find_element(By.XPATH, fieldset_xpath)
        fieldset_device: str = fieldset.get_attribute("data-device-index")

        drop_down_xpath = '//select[@name="device"]'
        drop_down: WebElement = context.driver.find_element(By.XPATH, drop_down_xpath)

        url: str = context.driver.current_url
        Select(drop_down).select_by_value(fieldset_device)

        push_button_xpath = '//button[@type="submit" and contains(text(), "Send Me a Push")]'
        push_button: WebElement = context.driver.find_element(By.XPATH, push_button_xpath)

        push_button.click()
        return context.wait_for_url_changed(url)

    return intercept


def single_sign_on(driver: Chrome, *, callback: Optional[ViewCallback] = None) -> Optional[bool]:
    iteration = 0

    terminal = ViewTerminal()
    context = ViewContext(driver, terminal)

    on_track: bool = True
    probers: List[ViewProber] = [probe_auth0, probe_databricks, probe_duo, probe_microsoft]

    while on_track is True:
        on_track = False

        if context.wait_for_network_idle() is False:
            return False

        iteration = iteration + 1
        driver.save_screenshot(f"current-{iteration:02}.png")

        if callback and callback(context):
            return True

        for prober in probers:
            if interceptor := prober(context):
                if on_track := interceptor():
                    break

    return on_track if True else None
