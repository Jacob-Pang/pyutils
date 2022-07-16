import os
import time

from pathlib import Path
from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.action_chains import ActionChains
from collections.abc import Iterable

DRIVER_DPATH = os.path.join(Path(__file__).parent.parent.absolute(), "drivers")

def auto_update_driver(method: callable):
    def wrapped_method(self, *args, **kwargs) -> any:
        try:    return method(self, *args, **kwargs)
        except: pass

        self.update_driver()
        return method(self, *args, **kwargs)

    return wrapped_method

def wrap_methods(wrapped_object: any, method_wrapper: callable, skip_methods: Iterable = set(),
    *args, **kwargs) -> None:
    for method_name in dir(wrapped_object):
        try:    method = getattr(wrapped_object, method_name)
        except: continue

        if method_name[0] == "_" or not callable(method):
            continue # protected method

        if method in skip_methods: continue
        setattr(wrapped_object, method_name, method_wrapper(method, *args, **kwargs))

def busy_waiting_execution(method, wrap_output_types: any = None) -> callable:
    def wrapped_method(*args, timeout = 5., request_freq = .5, **kwargs):
        execution_state = False
        exception_trace = None

        while timeout > 0:
            pre_request_time = time.time()

            try:
                output = method(*args, **kwargs)
                execution_state = True
                break
            except Exception as exception:
                exception_trace = exception
                time.sleep(request_freq)    

                timeout = timeout - request_freq - (time.time() - pre_request_time)

        if not execution_state:
            raise exception_trace

        if not wrap_output_types is None and isinstance(output, wrap_output_types):
            wrap_methods(output, busy_waiting_execution,
                    wrap_output_types=wrap_output_types)

        return output
    
    return wrapped_method

class WebsurferBase(webdriver.Chrome, webdriver.Firefox):
    @auto_update_driver
    def __init__(self, driver, service_construct, options, *option_args,
        preferences: dict = None) -> None:

        for arg in option_args:
            options.add_argument(arg)

        if preferences:
            options.add_experimental_option(
                "prefs", preferences
            )

        driver.__init__(self, service=service_construct(self.driver_path()), options=options)
        wrap_methods(self, busy_waiting_execution, skip_methods={self.get},
                wrap_output_types=WebElement) # Get method known to be buggy post-wrapping.

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return self.quit()

    def driver_path(self):
        return os.path.join(DRIVER_DPATH, f"{self.driver_executable_name()}.exe")

    def driver_executable_name(self) -> str:
        raise NotImplementedError()

    def update_driver(self):
        raise NotImplementedError()
    
    def pause(self, sleep):
        time.sleep(sleep)

    def find_from_downloads(self, file_name: str) -> str:
        directory = Path(os.path.join(Path.home(), "Downloads"))

        for path in directory.glob(file_name):
            return path
        
        raise Exception(f"No match found for {file_name}")

    def read_from_downloads(self, file_name: str, file_reader):
        file_path = self.find_from_downloads(file_name)

        if os.access(file_path, os.R_OK):
            return file_reader(file_path)
        
        raise Exception(f"PermissionError for file_path: {file_path}")

    def remove_from_downloads(self, file_name: str):
        directory = Path(os.path.join(Path.home(), "Downloads"))

        for path in directory.glob(file_name):
            os.remove(path)

    def body_height(self):
        return self.execute_script("return document.body.scrollHeight")

    def move_to_element(self, element: WebElement):
        ActionChains(self).move_to_element(element).perform()

    def move_to_end(self):
        height = self.body_height()

        while True:
            self.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(.5)
            
            new_height = self.body_height()

            if new_height == height:
                return

            height = new_height

    def click_element(self, web_element: WebElement) -> None:
        self.execute_script("arguments[0].click();", web_element)

if __name__ == "__main__":
    pass