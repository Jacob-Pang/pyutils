import rpa

from .. import WebSurferBase, Identifier
from .manager import RPAManager
from .manager import rpa_manager

class RPAWebSurfer (WebSurferBase):
    def __init__(self, visual_automation: bool = False, chrome_browser: bool = True,
        headless_mode: bool = False, turbo_mode: bool = False, rpa_manager: RPAManager = rpa_manager,
        rpa_instance_id: int = None, chrome_scan_period: int = rpa_manager._chrome_scan_period_def,
        sleeping_period: int = rpa_manager._sleeping_period_def, engine_scan_period: int =
        rpa_manager._engine_scan_period_def, incognito_mode: bool = False):

        WebSurferBase.__init__(self, headless_mode=headless_mode)
        self.rpa_manager = rpa_manager
        self.rpa: rpa = self.rpa_manager.get_rpa_instance(rpa_instance_id)

        # Setting RPA config
        self.rpa_manager.set_delay_config(self.rpa, chrome_scan_period=chrome_scan_period,
                sleeping_period=sleeping_period, engine_scan_period=engine_scan_period)

        self.rpa_manager.set_flags(self.rpa, incognito_mode=incognito_mode)

        self.rpa.init(visual_automation=visual_automation, chrome_browser=chrome_browser,
                headless_mode=headless_mode, turbo_mode=turbo_mode)

        self.visual_automation = visual_automation
        self.chrome_browser = chrome_browser
        self.turbo_mode = turbo_mode

    def get(self, url: str) -> None:
        self.rpa.url(url)
    
    def page_source(self) -> str:
        # Reading may return empty
        for _ in range(20):
            source = self.rpa.read("page")
            if source: break

        if not source:
            raise Exception("Could not retrieve the HTML from webpage.")

        return source

    def exists(self, element_identifier: Identifier, **kwargs) -> str:
        return self.rpa.exist(element_identifier.as_xpath())

    def restart(self) -> None:
        self.rpa.close()
        self.rpa.init(visual_automation=self.visual_automation, chrome_browser=self.chrome_browser,
                headless_mode=self.headless_mode, turbo_mode=self.turbo_mode)

    def close(self) -> None:
        rpa_manager.destroy_rpa_instance(self.rpa)

    def click_element(self, element_identifier: Identifier, **kwargs) -> None:
        self.rpa.click(element_identifier.__str__())

    def hover_over_element(self, element_identifier: Identifier, **kwargs) -> None:
        self.rpa.hover(element_identifier.as_xpath())

    def select_option(self, element_identifier: Identifier, option_value: any, **kwargs) -> None:
        self.rpa.select(element_identifier.as_xpath(), option_value)

    def get_text(self, element_identifier: Identifier) -> str:
        return self.rpa.read(element_identifier.as_xpath())

    def input_text(self, element_identifier: Identifier, text: str,
        send_enter_key: bool = False, **kwargs) -> None:
        
        if send_enter_key: text = f"{text}[enter]"
        self.rpa.type(element_identifier.__str__(), text)

if __name__ == "__main__":
    pass