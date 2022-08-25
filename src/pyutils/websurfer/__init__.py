import time
from pyutils import WrappedFunction

# Element Identifiers
class Identifier:
    def __init__(self, expression: str) -> None:
        self.expression = expression

    def __str__(self) -> str:
        return self.expression

class XPathIdentifier (Identifier):
    pass

class CssSelectorIdentifier (Identifier):
    pass

class WebsurferBase:
    @classmethod
    def initializer(cls: type, **kwargs) -> callable:
        return WrappedFunction(cls, **kwargs)

    def __init__(self, headless_mode: bool = False) -> None:
        self.headless = headless_mode

    def get(self, url: str) -> None:
        raise NotImplementedError()

    def page_source(self) -> str:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    def wait(self, seconds: int) -> None:
        time.sleep(seconds)

    def click_element(self, element_identifier: Identifier, **kwargs) -> None:
        raise NotImplementedError()

    def input_text(self, element_identifier: Identifier, text: str,
        send_enter_key: bool = False, **kwargs) -> None:
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return self.close()

if __name__ == "__main__":
    pass