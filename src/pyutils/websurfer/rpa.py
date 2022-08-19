import rpa
from pyutils.websurfer import WebsurferBase, Identifier

class RPAWebSurfer (WebsurferBase):
    def __init__(self, visual_automation: bool = False, chrome_browser: bool = True,
        headless_mode: bool = False, turbo_mode: bool = False):

        WebsurferBase.__init__(self, headless_mode=headless_mode)
        rpa.init(visual_automation=visual_automation, chrome_browser=chrome_browser,
                headless_mode=headless_mode, turbo_mode=turbo_mode)

    def get(self, url: str) -> None:
        rpa.url(url)
    
    def page_source(self) -> str:
        return rpa.read("page")
    
    def close(self) -> None:
        rpa.close()

    def click_element(self, element_identifier: Identifier, **kwargs) -> None:
        rpa.click(element_identifier.__str__())

    def input_text(self, element_identifier: Identifier, text: str,
        send_enter_key: bool = False, **kwargs) -> None:
        
        if send_enter_key: text = f"{text}[enter]"
        rpa.type(element_identifier.__str__(), text)

if __name__ == "__main__":
    pass