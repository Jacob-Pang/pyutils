from pyutils.websurfer import WebsurferBase, Identifier
from pyutils.websurfer.rpa.handler import get_rpa_instance, set_delays

class RPAWebSurfer (WebsurferBase):
    def __init__(self, visual_automation: bool = False, chrome_browser: bool = True,
        headless_mode: bool = False, turbo_mode: bool = False, **delay_kwargs):

        WebsurferBase.__init__(self, headless_mode=headless_mode)
        self.rpa = get_rpa_instance()
        set_delays(self.rpa, **delay_kwargs)

        self.rpa.init(visual_automation=visual_automation, chrome_browser=chrome_browser,
                headless_mode=headless_mode, turbo_mode=turbo_mode)

    def get(self, url: str) -> None:
        self.rpa.url(url)
    
    def page_source(self) -> str:
        return self.rpa.read("page")
    
    def get_text(self, element_identifier: Identifier) -> str:
        return self.rpa.read(element_identifier.as_xpath())

    def close(self) -> None:
        self.rpa.close()
        set_delays(self.rpa) # Reset

    def click_element(self, element_identifier: Identifier, **kwargs) -> None:
        self.rpa.click(element_identifier.__str__())

    def input_text(self, element_identifier: Identifier, text: str,
        send_enter_key: bool = False, **kwargs) -> None:
        
        if send_enter_key: text = f"{text}[enter]"
        self.rpa.type(element_identifier.__str__(), text)

if __name__ == "__main__":
    pass