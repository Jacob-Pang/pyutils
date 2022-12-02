from pyutils.websurfer import WebsurferBase, Identifier
from pyutils.websurfer.rpa.handler import get_rpa_instance, set_delays

class RPAWebSurfer (WebsurferBase):
    def __init__(self, visual_automation: bool = False, chrome_browser: bool = True,
        headless_mode: bool = False, turbo_mode: bool = False, **delay_config):

        WebsurferBase.__init__(self, headless_mode=headless_mode)
        self.rpa = get_rpa_instance()
        set_delays(self.rpa, **delay_config)
        self.rpa.init(visual_automation=visual_automation, chrome_browser=chrome_browser,
                headless_mode=headless_mode, turbo_mode=turbo_mode)

        self.visual_automation = visual_automation
        self.chrome_browser = chrome_browser
        self.turbo_mode = turbo_mode

    def get(self, url: str) -> None:
        self.rpa.url(url)
    
    def page_source(self) -> str:
        # Bug found where reading returns empty
        for _ in range(20):
            source = self.rpa.read("page")
            if source: break

        if not source:
            raise Exception("Could not retrieve the HTML from webpage.")

        return source
    
    def get_text(self, element_identifier: Identifier) -> str:
        return self.rpa.read(element_identifier.as_xpath())

    def restart(self) -> None:
        self.rpa.close()
        self.rpa.init(visual_automation=self.visual_automation, chrome_browser=self.chrome_browser,
                headless_mode=self.headless, turbo_mode=self.turbo_mode)

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