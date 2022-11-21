import json
import os

def make_cython_templates(template_json_fpath: str) -> list[str]:
    """ Generates specialized cython modules based on templates and returns the
    source files (.pyx).
    
    todo documentation
    """
    template_dpath = os.path.dirname(template_json_fpath)

    def write_file(fpath: str, content: str) -> None:
        if os.path.exists(fpath):
            os.remove(fpath)

        with open(fpath, "w") as file:
            file.write(content)

    with open(template_json_fpath) as json_file:
        templates = json.load(json_file)

    source_files = []

    for module_name in templates.keys():
        pxd_template_fpath = os.path.join(template_dpath, f"{module_name}.pxd_template")
        pyx_template_fpath = os.path.join(template_dpath, f"{module_name}.pyx_template")

        with open(pxd_template_fpath, "r") as pxd_file:
            pxd_template = pxd_file.read()

        with open(pyx_template_fpath, "r") as pyx_file:
            pyx_template = pyx_file.read()

        module_py = ""

        for specialized_module_name, config in templates.get(module_name).items():
            specialized_pxd = pxd_template
            specialized_pyx = pyx_template

            for find_key, replace_key in config.items():
                specialized_pxd = specialized_pxd.replace(find_key, replace_key)
                specialized_pyx = specialized_pyx.replace(find_key, replace_key)

            specialized_pxd_fpath = os.path.join(template_dpath, f"{specialized_module_name}.pxd")
            specialized_pyx_fpath = os.path.join(template_dpath, f"{specialized_module_name}.pyx")
            write_file(specialized_pxd_fpath, specialized_pxd)
            write_file(specialized_pyx_fpath, specialized_pyx)
            source_files.append(specialized_pyx_fpath)

            module_py += f"from .{specialized_module_name} import *\n"

        module_py_fpath = os.path.join(template_dpath, f"{module_name}.py")
        write_file(module_py_fpath, module_py)

    return source_files

if __name__ == "__main__":
    pass