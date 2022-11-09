from selenium.webdriver.common.by import By
from pyutils.websurfer import WebsurferBase
from pyutils.websurfer import CssSelectorIdentifier, Identifier, XPathIdentifier
from pyutils.websurfer.selenium_ext import WebdriverBase
from pyutils.websurfer.selenium_ext.chrome import ChromeWebdriver

class SeleniumWebSurfer (WebsurferBase):
    def __init__(self, headless_mode: bool = False, webdriver: WebdriverBase = ChromeWebdriver,
        *option_args, preferences: dict = None):
        WebsurferBase.__init__(self, headless_mode=headless_mode)
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