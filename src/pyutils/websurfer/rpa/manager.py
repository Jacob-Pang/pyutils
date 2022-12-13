import os
import platform
import re
import rpa
import sys

from types import ModuleType
from importlib.util import spec_from_file_location, module_from_spec
from multiprocessing import Semaphore
from multiprocessing.managers import SyncManager

cloned_module_directory = os.path.dirname(rpa.__file__)
cloned_source_directory = None

def get_remote_debugging_port(rpa_instance_id: int) -> str:
    return str(9222 - rpa_instance_id)

def tagui_dirname() -> str:
    if platform.system() == "Windows":
        return "tagui"
    
    return ".tagui"

def get_rpa_clone(rpa_instance_id: int) -> ModuleType:
    rpa_clone_name = f"tagui_clone_{rpa_instance_id:0>2}"
    tagui_py_fpath = os.path.join(os.path.dirname(rpa.__file__), f"tagui.py")
    tagui_clone_py_fpath = os.path.join(cloned_module_directory, f"{rpa_clone_name}.py")

    if not os.path.exists(tagui_clone_py_fpath):
        with open(tagui_py_fpath, "r") as file:
            program = file.read()

        # Change temp file names to prevent conflict in read-write operations
        program = program.replace("'rpa_python'", f"'rpa_python_{rpa_instance_id:0>2}'")
        program = program.replace("' rpa_python '", f"' rpa_python_{rpa_instance_id:0>2} '")
        program = program.replace("rpa_python.txt", f"rpa_python_{rpa_instance_id:0>2}.txt")
        program = program.replace("'rpa_python.log'", f"'rpa_python_{rpa_instance_id:0>2}.log'")
        program = program.replace("'rpa_python.js'", f"'rpa_python_{rpa_instance_id:0>2}.js'")
        program = program.replace("'rpa_python.raw'", f"'rpa_python_{rpa_instance_id:0>2}.raw'")

        # Comment out ending of existing processes
        program = program.replace("os.system('\"' + end_processes_executable + '\"')",
                "# os.system('\"' + end_processes_executable + '\"')")
        
        with open(tagui_clone_py_fpath, "w") as file:
            file.write(program)

    # Dynamically import rpa_clone module   
    rpa_clone_spec = spec_from_file_location(rpa_clone_name, tagui_clone_py_fpath)
    rpa_clone = module_from_spec(rpa_clone_spec)
    sys.modules[rpa_clone_name] = rpa_clone
    rpa_clone_spec.loader.exec_module(rpa_clone)

    tagui_clone_dpath = os.path.join(cloned_source_directory if cloned_source_directory else
            rpa.tagui_location(), rpa_clone_name)
    
    rpa_clone.tagui_location(tagui_clone_dpath)
    
    if not os.path.exists(tagui_clone_dpath):
        os.mkdir(tagui_clone_dpath)
        rpa_clone.setup()

        # overwrite tagui.cmd to prevent port conflicts
        tagui_cmd_fpath = os.path.join(tagui_clone_dpath, tagui_dirname(), "src", "tagui.cmd")

        with open(tagui_cmd_fpath, "r") as file:
            program = file.read()

        program = program.replace("9222", get_remote_debugging_port(rpa_instance_id))

        os.remove(tagui_cmd_fpath)

        with open(tagui_cmd_fpath, "w") as file:
            file.write(program)

    setattr(rpa_clone, "tagui_dpath", tagui_clone_dpath)
    return rpa_clone

def end_chrome_process(rpa_instance: rpa) -> None:
    end_chrome_cmd = os.path.join(rpa_instance.tagui_location(), tagui_dirname(),
            "src", "end_chrome.cmd")

    if not os.path.exists(end_chrome_cmd):
        # Generate end_chrome command file
        template_fpath = os.path.join(os.path.dirname(__file__), "end_chrome_template.cmd")

        with open(template_fpath, "r") as file:
            program = file.read()

        program = program.replace("9222", get_remote_debugging_port(rpa_instance.rpa_instance_id))

        with open(end_chrome_cmd, "w") as file:
            file.write(program)
    
    os.system('"' + end_chrome_cmd + '"')

# Optimization method to speed up rpa
def set_delays(rpa_instance: rpa, chrome_scan_period: int = 100000, looping_delay: bool = True,
    sleep_period: int = 500, engine_scan_period: int = .5) -> None:
    
    # From https://github.com/tebelorg/RPA-Python/issues/120
    tagui_dpath = rpa_instance.tagui_location()
    tagui_chrome_fpath = os.path.join(tagui_dpath, tagui_dirname(), "src", "tagui_chrome.php")
    tagui_header_fpath = os.path.join(tagui_dpath, tagui_dirname(), "src", "tagui_header.js")
    tagui_sikuli_fpath = os.path.join(tagui_dpath, tagui_dirname(), "src", "tagui.sikuli", "tagui.py")

    if not os.path.exists(tagui_chrome_fpath):
        # Binaries and files not downloaded
        rpa_instance.setup()

    # modify tagui_chrome.php
    with open(tagui_chrome_fpath, "r") as file:
        program = file.read()

    program = re.sub("scan_period = \d+;", f"scan_period = {chrome_scan_period};", program)
    os.remove(tagui_chrome_fpath)

    with open(tagui_chrome_fpath, "w") as file:
        file.write(program)

    # modify tagui_header.js
    with open(tagui_header_fpath, "r") as file:
        program = file.read()

    program = re.sub("function sleep\(ms\) .*\n",
        "function sleep(ms) { // helper to add delay during loops\n" if looping_delay else
        "function sleep(ms) { return; // helper to add delay during loops\n",
        program
    )

    program = re.sub("sleep\(\d+\)", f"sleep({sleep_period})", program)
    os.remove(tagui_header_fpath)

    with open(tagui_header_fpath, "w") as file:
        file.write(program)

    # modify tagui.sikuli/tagui.py
    with open(tagui_sikuli_fpath, "r") as file:
        program = file.read()

    program = re.sub("scan_period = \d+", f"scan_period = {engine_scan_period}", program)
    os.remove(tagui_sikuli_fpath)

    with open(tagui_sikuli_fpath, "w") as file:
        file.write(program)

class RPAManager:
    rpa_instances = dict()
    semaphore = Semaphore(1)

    def get_rpa_instance(self) -> ModuleType:
        with self.semaphore:
            rpa_instance_id = 0

            while rpa_instance_id in self.rpa_instances:
                rpa_instance_id += 1
            
            self.rpa_instances[rpa_instance_id] = 0

        rpa_instance = get_rpa_clone(rpa_instance_id) if rpa_instance_id else rpa
        setattr(rpa_instance, "rpa_instance_id", rpa_instance_id)

        return rpa_instance
    
    def destroy_rpa_instance(self, rpa_instance: rpa) -> None:
        # Closes the rpa and recycles the use of the rpa_instance
        rpa_instance.close()
        end_chrome_process(rpa_instance) # Purge zombie chrome processes
        set_delays(rpa_instance) # Resets settings

        with self.semaphore:
            self.rpa_instances.pop(rpa_instance.rpa_instance_id)

rpa_manager = RPAManager()

def sync_rpa_manager(sync_manager: SyncManager) -> None:
    global rpa_manager

    rpa_manager.rpa_instances = sync_manager.dict()
    rpa_manager.semaphore = sync_manager.Semaphore(1)

# Setters
def set_rpa_source_directory(dir_path: str) -> None:
    rpa.tagui_location(dir_path)

def set_cloned_module_directory(dir_path: str) -> None:
    global cloned_module_directory
    cloned_module_directory = dir_path

def set_cloned_source_directory(dir_path: str) -> None:
    global cloned_source_directory
    cloned_source_directory = dir_path

# Removers
def destroy_clones() -> None:
    # Todo
    pass

if __name__ == "__main__":
    pass