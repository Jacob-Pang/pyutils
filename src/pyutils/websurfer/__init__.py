from lxml import etree

# Element Identifiers
class Identifier:
    def __init__(self, expression: str) -> None:
        self.expression = expression

    def __str__(self) -> str:
        return self.expression

    def as_xpath(self) -> str:
        raise NotImplementedError()

    def as_css(self) -> str:
        raise NotImplementedError()
    
    def get_child(self, expression_extension: str) -> "Identifier":
        raise NotImplementedError()

class XPathIdentifier (Identifier):
    def as_xpath(self) -> str:
        return self.__str__()

    def get_child(self, extension: str) -> "Identifier":
        return XPathIdentifier(f"{self.expression}/{extension}")

class CssSelectorIdentifier (Identifier):
    def as_css(self) -> str:
        return self.__str__()

class WebsurferBase:
    def __init__(self, headless_mode: bool = False) -> None:
        self.headless_mode = headless_mode

    def get(self, url: str) -> None:
        raise NotImplementedError()

    def page_source(self) -> str:
        # Returns the page HTML
        raise NotImplementedError()

    def restart(self) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    def exists(self, element_identifier: Identifier, **kwargs) -> str:
        # Returns whether the element exists
        raise NotImplementedError()

    def get_text(self, element_identifier: Identifier, **kwargs) -> str:
        # Returns the text for the first match
        raise NotImplementedError()

    def find_elements(self, element_identifier: Identifier, **kwargs) -> list:
        # Returns matching elements
        return etree.HTML(self.page_source()).xpath(element_identifier.as_xpath())

    def click_element(self, element_identifier: Identifier, **kwargs) -> None:
        raise NotImplementedError()

    def input_text(self, element_identifier: Identifier, text: str, send_enter_key:
        bool = False, **kwargs) -> None:
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return self.close()

class PageRenderedPredicate:
    # Checks whether there were changes to the webpage source.
    def __init__(self, websurfer: WebsurferBase):
        self.websurfer = websurfer
        self.page_source = self.websurfer.page_source()
    
    def __call__(self) -> bool:
        page_source = self.websurfer.page_source()

        if page_source == self.page_source:
            return True

        self.page_source = page_source
        return False

if __name__ == "__main__":
    pass