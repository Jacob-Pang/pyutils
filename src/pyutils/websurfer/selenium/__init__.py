from selenium.webdriver.common.by import By
from .webdriver import WebdriverBase
from .chrome import ChromeWebdriver
from .. import WebSurferBase
from .. import CssSelectorIdentifier, Identifier, XPathIdentifier

class SeleniumWebSurfer (WebSurferBase):
    def __init__(self, headless_mode: bool = False, webdriver: WebdriverBase = ChromeWebdriver,
        *option_args, preferences: dict = None):
        WebSurferBase.__init__(self, headless_mode=headless_mode)
        self.webdriver = webdriver("--headless", *option_args, preferences=preferences) \
                if headless_mode else webdriver(*option_args, preferences=preferences)

    def get(self, url: str) -> None:
        self.webdriver.get(url)
    
    def page_source(self) -> str:
        return self.webdriver.page_source
    
    def close(self) -> None:
        self.webdriver.quit()

    def click_element(self, element_identifier: Identifier, **kwargs) -> None:
        if isinstance(element_identifier, XPathIdentifier):
            self.webdriver.find_element(By.XPATH, element_identifier.as_xpath(), **kwargs).click()
        elif isinstance(element_identifier, CssSelectorIdentifier):
            self.webdriver.find_element(By.CSS_SELECTOR, element_identifier.as_css(), **kwargs).click()

if __name__ == "__main__":
    pass