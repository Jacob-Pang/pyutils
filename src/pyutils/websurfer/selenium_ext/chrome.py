import os
import requests
import wget
import zipfile

from selenium import webdriver
from pyutils.websurfer.selenium_ext import WebdriverBase

class ChromeWebdriver(WebdriverBase):
    @staticmethod
    def default_headless_option_args() -> tuple:
        return "--headless", "--disable-gpu", "--window-size=1920,1200", "--ignore-certificate-errors", \
            "--disable-extensions", "--no-sandbox", "--disable-dev-shm-usage"

    def __init__(self, *option_args, preferences: dict = None):
        WebdriverBase.__init__(
            self, webdriver.Chrome,
            webdriver.chrome.service.Service,
            webdriver.ChromeOptions(),
            *option_args,
            preferences=preferences
        )

    def driver_executable_name(self) -> str:
        return "chromedriver"

    @staticmethod
    def update_driver(driver_dpath: str, version = None, os_type: str = "win32"):
        if not version:
            version = requests.get("https://chromedriver.storage.googleapis.com/LATEST_RELEASE").text
        
        chromedriver_zip = wget.download(
            f"https://chromedriver.storage.googleapis.com/{version}/chromedriver_{os_type}.zip",
            "chromedriver.zip"
        )

        if not os.path.exists(driver_dpath):
            os.makedirs(driver_dpath)

        with zipfile.ZipFile(chromedriver_zip, "r") as zip:
            zip.extractall(driver_dpath)

        os.remove(chromedriver_zip)

if __name__ == "__main__":
    pass
