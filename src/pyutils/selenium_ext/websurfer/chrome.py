import os
import requests
import wget
import zipfile

from selenium import webdriver

from pyutils.selenium_ext.websurfer import DRIVER_DPATH
from pyutils.selenium_ext.websurfer import WebsurferBase

class ChromeSurfer(WebsurferBase):
    @staticmethod
    def default_headless_option_args() -> tuple:
        return "--headless", "--disable-gpu", "--window-size=1920,1200", "--ignore-certificate-errors", \
            "--disable-extensions", "--no-sandbox", "--disable-dev-shm-usage"
        
    def __init__(self, *option_args, preferences: dict = None):
        WebsurferBase.__init__(
            self, webdriver.Chrome,
            webdriver.chrome.service.Service,
            webdriver.ChromeOptions(),
            *option_args,
            preferences=preferences
        )

    def driver_executable_name(self) -> str:
        return "chromedriver"

    @staticmethod
    def update_driver(version = None, os_type: str = "win32"):
        if not version:
            version = requests.get("https://chromedriver.storage.googleapis.com/LATEST_RELEASE").text
        
        chromedriver_zip = wget.download(
            f"https://chromedriver.storage.googleapis.com/{version}/chromedriver_{os_type}.zip",
            "chromedriver.zip"
        )

        if not os.path.exists(DRIVER_DPATH):
            os.makedirs(DRIVER_DPATH)

        with zipfile.ZipFile(chromedriver_zip, "r") as zip:
            zip.extractall(DRIVER_DPATH)

        os.remove(chromedriver_zip)

if __name__ == "__main__":
    pass
