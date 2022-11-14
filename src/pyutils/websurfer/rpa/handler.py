import inspect
import os
import re
import rpa
import shutil

from importlib import import_module
from types import ModuleType

rpa_instances = 0

def chdir_wrapper(method: callable, dirname: str) -> callable:
    def wrapped(*args, **kwargs) -> any:
        cwd = os.getcwd()

        # check if directory has been changed already.
        if os.path.basename(cwd) == dirname:
            return method(*args, **kwargs)

        nwd = os.path.join(cwd, dirname)

        if not os.path.exists(nwd):
            os.mkdir(nwd)
        
        os.chdir(nwd)
        out = method(*args, **kwargs)
        os.chdir(cwd)

        return out

    return wrapped

def rmdir_wrapper(method: callable, dirname: str) -> callable:
    def wrapped(*args, **kwargs):
        cwd = os.getcwd()

        # check if directory has been changed already.
        if os.path.basename(cwd) == dirname:
            return method(*args, **kwargs)
        
        nwd = os.path.join(cwd, dirname)
        out = method(*args, **kwargs)
        shutil.rmtree(nwd)

        return out

    return wrapped


def make_rpa_clone() -> ModuleType:
    global rpa_instances

    clone_name = f"tagui_clone_{rpa_instances:0>2}"
    tagui_py_fpath = os.path.join(os.path.dirname(rpa.__file__), f"tagui.py")
    tagui_clone_py_fpath = os.path.join(os.path.dirname(rpa.__file__), f"{clone_name}.py")

    if not os.path.exists(tagui_clone_py_fpath):
        with open(tagui_py_fpath, "r") as file:
            program = file.read()

        # Comment out ending of existing processes
        program = program.replace("os.system('\"' + end_processes_executable + '\"')",
                "# os.system('\"' + end_processes_executable + '\"')")
        
        with open(tagui_clone_py_fpath, "w") as file:
            file.write(program)

    rpa_clone = import_module(clone_name)
    tagui_clone_dpath = os.path.join(rpa.tagui_location(), clone_name)
    rpa_clone.tagui_location(tagui_clone_dpath)
    
    if not os.path.exists(tagui_clone_dpath):
        os.mkdir(tagui_clone_dpath)
        rpa_clone.setup()

        # overwrite tagui.cmd to prevent port conflicts
        tagui_cmd_fpath = os.path.join(tagui_clone_dpath, "tagui", "src", "tagui.cmd")

        with open(tagui_cmd_fpath, "r") as file:
            program = file.read()

        program = program.replace("9222", str(9222 - rpa_instances))

        os.remove(tagui_cmd_fpath)

        with open(tagui_cmd_fpath, "w") as file:
            file.write(program)

    # wrap clone methods
    for name, method in inspect.getmembers(rpa_clone, inspect.isfunction):
        setattr(rpa_clone, name, chdir_wrapper(method, clone_name))

    rpa_clone.close = rmdir_wrapper(rpa_clone.close, clone_name)

    return rpa_clone

def get_rpa_instance() -> ModuleType:
    # Init and download directories must be created prior to use for clones
    global rpa_instances

    if not rpa_instances:
        rpa_instances += 1
        return rpa

    # create clone
    rpa_clone = make_rpa_clone()
    rpa_instances += 1

    return rpa_clone

def destroy_clones() -> None:
    # Todo
    pass

# Optimizations
def set_delays(rpa_instance: rpa, chrome_scan_period: int = 100000, looping_delay: bool = True,
    sleep_period: int = 500, engine_scan_period: int = .5) -> None:
    
    # From https://github.com/tebelorg/RPA-Python/issues/120
    tagui_dpath = rpa_instance.tagui_location()
    tagui_chrome_fpath = os.path.join(tagui_dpath, "tagui", "src", "tagui_chrome.php")
    tagui_header_fpath = os.path.join(tagui_dpath, "tagui", "src", "tagui_header.js")
    tagui_sikuli_fpath = os.path.join(tagui_dpath, "tagui", "src", "tagui.sikuli", "tagui.py")

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

if __name__ == "__main__":
    pass