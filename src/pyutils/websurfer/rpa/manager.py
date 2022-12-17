import os
import platform
import re
import rpa
import sys

from types import ModuleType
from multiprocessing import Semaphore
from multiprocessing.managers import SyncManager
from importlib.util import spec_from_file_location, module_from_spec

def get_remote_debugging_port(rpa_instance_id: int) -> str:
    return str(9222 - rpa_instance_id)

def get_tagui_folder_name() -> str:
    if platform.system() == "Windows":
        return "tagui"
    
    return ".tagui"

def get_tagui_cmd_fpath(rpa_instance: rpa) -> str:
    return os.path.join(rpa_instance.tagui_location(), get_tagui_folder_name(),
            "src", "tagui.cmd")

def get_tagui_chrome_fpath(rpa_instance: rpa) -> str:
    return os.path.join(rpa_instance.tagui_location(), get_tagui_folder_name(),
            "src", "tagui_chrome.php")

def get_tagui_header_fpath(rpa_instance: rpa) -> str:
    return os.path.join(rpa_instance.tagui_location(), get_tagui_folder_name(),
            "src", "tagui_header.js")

def get_tagui_sikuli_fpath(rpa_instance: rpa) -> str:
    return os.path.join(rpa_instance.tagui_location(), get_tagui_folder_name(),
            "src", "tagui.sikuli", "tagui.py")

def get_end_chrome_cmd_fpath(rpa_instance: rpa) -> str:
    return os.path.join(rpa_instance.tagui_location(), get_tagui_folder_name(),
            "src", "end_chrome.cmd")

def end_chrome_process(rpa_instance: rpa) -> None:
    end_chrome_cmd_fpath = get_end_chrome_cmd_fpath(rpa_instance)

    if not os.path.exists(end_chrome_cmd_fpath):
        # Generate end_chrome command file
        template_fpath = os.path.join(os.path.dirname(__file__), "end_chrome_template.cmd")

        with open(template_fpath, "r") as file:
            program = file.read()

        program = program.replace("9222", get_remote_debugging_port(rpa_instance.rpa_instance_id))

        with open(end_chrome_cmd_fpath, "w") as file:
            file.write(program)
    
    os.system('"' + end_chrome_cmd_fpath + '"')

class RPAManager:
    _cloned_source_dpath = None

    @staticmethod
    def set_rpa_source_dpath(dpath: str) -> None:
        rpa.tagui_location(dpath)

    @staticmethod
    def set_delay_config(rpa_instance: rpa, chrome_scan_period: int = 100000, looping_delay: bool = True,
        sleep_period: int = 500, engine_scan_period: int = .5) -> None:
        """
        Sources:
            https://github.com/tebelorg/RPA-Python/issues/120
        """
        tagui_chrome_fpath = get_tagui_chrome_fpath(rpa_instance)
        tagui_header_fpath = get_tagui_header_fpath(rpa_instance)
        tagui_sikuli_fpath = get_tagui_sikuli_fpath(rpa_instance)

        if not os.path.exists(tagui_chrome_fpath):
            # Not setup: missing binaries
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

    @staticmethod
    def set_flags(rpa_instance: rpa, incognito_mode: bool = False) -> None:
        """
        Sources:
            https://github.com/tebelorg/RPA-Python/issues/123
        """
        tagui_cmd_fpath = get_tagui_cmd_fpath(rpa_instance)

        if not os.path.exists(tagui_cmd_fpath):
            # Not setup: missing binaries
            rpa_instance.setup()

        with open(tagui_cmd_fpath, 'r') as file:
            tagui_cmd_prog = file.read()

        for prefix, config, suffix in re.findall(r"(chrome_switches=)([^\n]*)(--remote-debugging-port)",
            tagui_cmd_prog):

            origin_flags = prefix + config + suffix
            modified_config: str = config

            if incognito_mode and "--incognito" not in modified_config:
                modified_config += " --incognito "
            elif not incognito_mode and "--incognito" in modified_config:
                modified_config = modified_config.replace("--incognito", '')
            
            # Remove consecutive whitespaces
            modified_flags = re.sub(r"\s+", ' ', prefix + modified_config + suffix)
            tagui_cmd_prog = tagui_cmd_prog.replace(origin_flags, modified_flags)

        os.remove(tagui_cmd_fpath)

        with open(tagui_cmd_fpath, 'w') as file:
            file.write(tagui_cmd_prog)

    def __init__(self, rpa_instance_map: dict[int, ModuleType] = dict(),
        locking_files_dpath: str = os.path.join(os.path.dirname(rpa.__file__), "rpa_manager_temp_files"),
        cloned_module_dpath: str = os.path.dirname(rpa.__file__), cloned_source_dpath: str = None) -> None:

        self.rpa_instances = rpa_instance_map
        self.semaphore = Semaphore(1)
        
        self.locking_files_dpath = locking_files_dpath
        self.cloned_module_dpath = cloned_module_dpath
        self.cloned_source_dpath = cloned_source_dpath
    
    @property
    def cloned_source_dpath(self) -> str:
        if self._cloned_source_dpath:
            return self._cloned_source_dpath
        
        return rpa.tagui_location()

    @cloned_source_dpath.setter
    def cloned_source_dpath(self, dpath: str) -> None:
        self._cloned_source_dpath = dpath

    # Setters
    def sync(self, sync_manager: SyncManager) -> None:
        self.rpa_instances = sync_manager.dict()
        self.semaphore = sync_manager.Semaphore(1)

    # Locking files methods
    def get_locking_file_path(self, rpa_instance_id: int) -> str:
        return os.path.join(self.locking_files_dpath, f"{rpa_instance_id}.lock")
    
    def locking_file_exists(self, rpa_instance_id: int) -> bool:
        return os.path.exists(self.get_locking_file_path(rpa_instance_id))

    def make_locking_file(self, rpa_instance_id: int) -> None:
        if not os.path.exists(self.locking_files_dpath):
            os.makedirs(self.locking_files_dpath)

        lock_file_path = self.get_locking_file_path(rpa_instance_id)

        if not os.path.exists(lock_file_path):
            lock_file = open(lock_file_path, 'w')
            lock_file.close()

    def destroy_lock_file(self, rpa_instance_id: int) -> None:
        if not os.path.exists(self.locking_files_dpath):
            return

        lock_file_path = self.get_locking_file_path(rpa_instance_id)

        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
        
        if len(os.listdir(self.locking_files_dpath)) == 0:
            os.rmdir(self.locking_files_dpath)
    
    # Instance constructor methods
    def remove_orphans(self) -> None:
        # rpa_instances without corresponding lock_files are orphaned.
        for rpa_instance_id in list[int](self.rpa_instances.keys()):
            if not self.locking_file_exists(rpa_instance_id):
                self.rpa_instances.pop(rpa_instance_id)

    def assign_rpa_instance_id(self) -> int:
        with self.semaphore:
            self.remove_orphans()
            rpa_instance_id = 0

            while True:
                lock_file_exists = self.locking_file_exists(rpa_instance_id)

                if not (rpa_instance_id in self.rpa_instances or lock_file_exists):
                    break

                if lock_file_exists:
                    # Instance assigned by another rpa_manager object *
                    self.rpa_instances[rpa_instance_id] = None
                
                rpa_instance_id += 1
            
            self.rpa_instances[rpa_instance_id] = None
            self.make_locking_file(rpa_instance_id)

            return rpa_instance_id

    def get_rpa_clone(self, rpa_instance_id: int) -> ModuleType:
        rpa_clone_name = f"tagui_clone_{rpa_instance_id:0>2}"
        tagui_py_fpath = os.path.join(os.path.dirname(rpa.__file__), f"tagui.py")
        tagui_clone_py_fpath = os.path.join(self.cloned_module_dpath, f"{rpa_clone_name}.py")

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

        tagui_clone_dpath = os.path.join(self.cloned_source_dpath, rpa_clone_name)
        rpa_clone.tagui_location(tagui_clone_dpath)
        
        if not os.path.exists(tagui_clone_dpath):
            os.mkdir(tagui_clone_dpath)
            rpa_clone.setup()
            tagui_cmd_fpath = get_tagui_cmd_fpath(rpa_clone)

            with open(tagui_cmd_fpath, "r") as file:
                program = file.read()

            # Change debugging port number
            program = program.replace("9222", get_remote_debugging_port(rpa_instance_id))

            os.remove(tagui_cmd_fpath)

            with open(tagui_cmd_fpath, "w") as file:
                file.write(program)

        setattr(rpa_clone, "tagui_dpath", tagui_clone_dpath)
        return rpa_clone

    def get_rpa_instance(self, rpa_instance_id: int = None) -> ModuleType:
        if rpa_instance_id is None:
            rpa_instance_id = self.assign_rpa_instance_id()

        rpa_instance = self.get_rpa_clone(rpa_instance_id) if rpa_instance_id else rpa
        setattr(rpa_instance, "rpa_instance_id", rpa_instance_id)

        # Generates entry if entry does not exist
        self.rpa_instances[rpa_instance_id] = rpa_instance
        return rpa_instance
    
    def destroy_rpa_instance(self, rpa_instance_or_id: (ModuleType | int)) -> None:
        # Closes the rpa and recycles the use of the rpa_instance
        rpa_instance, rpa_instance_id = (self.rpa_instances[rpa_instance_or_id], rpa_instance_or_id) \
                if isinstance(rpa_instance_or_id, int) else \
                (rpa_instance_or_id, rpa_instance_or_id.rpa_instance_id)

        if rpa_instance: # Purge zombie processes
            rpa_instance.close()
            end_chrome_process(rpa_instance)

            # Resets settings
            self.set_delay_config(rpa_instance)
            self.set_flags(rpa_instance)

        with self.semaphore:
            self.rpa_instances.pop(rpa_instance_id)
            self.destroy_lock_file(rpa_instance_id)

    # Destructors
    def remove_clones(self) -> None:
        raise NotImplementedError()

    def remove_lock_files(self) -> None:
        if not os.path.exists(self.locking_files_dpath):
            return

        for lock_file_name in os.listdir(self.locking_files_dpath):
            os.remove(os.path.join(self.locking_files_dpath, lock_file_name))
        
        os.rmdir(self.locking_files_dpath)

    def __del__(self) -> None:
        # Destructor method
        for rpa_instance_id in list[int](self.rpa_instances.keys()):
            self.destroy_rpa_instance(rpa_instance_id)

rpa_manager = RPAManager()

if __name__ == "__main__":
    pass