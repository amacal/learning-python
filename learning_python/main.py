import click

from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from learning_python.sso import ViewContext
from learning_python.sso import single_sign_on


@click.command()
def sso() -> None:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--user-data-dir=./profile")

    browser = Chrome(options=chrome_options)
    browser.set_window_size(1280, 720)
    browser.get("https://dbc-dna-e2-dev.cloud.databricks.com")

    def callback(context: ViewContext) -> bool:
        databricks = "https://dbc-dna-e2-dev.cloud.databricks.com"
        if not context.driver.current_url.startswith(databricks):
            return False

        xpath = f'//button[@data-testid="User.Dropdown"]'
        return len(context.driver.find_elements(By.XPATH, xpath)) > 0

    print(single_sign_on(browser, callback=callback))
    browser.quit()
