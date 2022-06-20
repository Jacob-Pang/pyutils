import __main__
import importlib
import inspect
import os
import pkgutil
import sys
import types
from pyutils.wrappers import RedirectIOStream
""" Dependency Notes:

Dependency(A, B) notates dependency of A on B; and thus B must be independent of A.
    definition of dependency:
        A cannot be imported without importing the definition of B.
    examples:
        - The module A or that which A is defined within, imports B or indirectly
                through C such that Dependency(C, B).
        - B is defined within the module A.

    * Modules, functions and classes defined in built-in or standard library modules
        are assumed to have no dependencies.
"""
def builtin_or_stdlib(module: types.ModuleType) -> bool:
    return module.__name__ in sys.builtin_module_names or module.__name__ in sys.stdlib_module_names

def graph_dependencies(module: types.ModuleType, dependency_graph: dict = dict(),
    skip_modules: set = set()) -> dict:

    def _graph_dependencies(module: types.ModuleType) -> None:
        if not module or builtin_or_stdlib(module) or module in skip_modules:
            return

        # Set new dependency node
        dependency_node = types.SimpleNamespace(defined_modules=set(), defined_classes=set(),
                defined_functions=set(), imported_modules=dict(), imported_classes=dict(),
                imported_functions=dict())

        dependency_graph[module] = dependency_node
        
        if hasattr(module, "__path__"): # Package: recurse through defined modules
            for _, module_name, _ in pkgutil.iter_modules(module.__path__):
                try:
                    defined_module = importlib.import_module(f".{module_name}", package=module.__name__)
                except: # Unable to import module
                    continue

                dependency_node.defined_modules.add(defined_module)
                _graph_dependencies(defined_module)
        else: # Recurse through dependencies by module imports
            for module_name, module_member in inspect.getmembers(module, inspect.ismodule):
                dependency_node.imported_modules[module_member] = module_name
                _graph_dependencies(module_member)

        # Recurse through dependencies by class definitions
        for class_name, class_member in inspect.getmembers(module, inspect.isclass):
            parent_module = inspect.getmodule(class_member)

            if parent_module == module:
                dependency_node.defined_classes.add(class_member)
                continue

            # Imported class definition
            dependency_node.imported_classes[class_member] = (class_name, parent_module)
            _graph_dependencies(parent_module)
        
        # Recurse through dependencies by class definitions
        for function_name, function_member in inspect.getmembers(module, inspect.isfunction):
            parent_module = inspect.getmodule(function_member)

            if parent_module == module:
                dependency_node.defined_functions.add(function_member)
                continue

            # Imported class definition
            dependency_node.imported_functions[function_member] = (function_name, parent_module)
            _graph_dependencies(parent_module)

        return dependency_graph

    # Silence warnings from imports
    with RedirectIOStream(stdout_dest=os.devnull, stderr_dest=os.devnull):
        _graph_dependencies(module)

    return dependency_graph

def has_dependency(obj: (types.ModuleType | types.FunctionType | object),
    dependency: (types.ModuleType | types.FunctionType | object), dependency_graph: dict,
    skip_modules: set = set()):
    # Returns the predicate Dependency(obj, dependency).
    if inspect.isfunction(obj) or inspect.isclass(obj):
        # Set the defining module as the subject of dependency
        return has_dependency(inspect.getmodule(obj), dependency, dependency_graph)
    
    if builtin_or_stdlib(obj): return False

    if not obj in dependency_graph:
        graph_dependencies(obj, dependency_graph)

    # Defined or imported dependency
    if inspect.ismodule(dependency) and (dependency in dependency_graph.get(obj).defined_modules or \
        dependency in dependency_graph.get(obj).imported_modules):
        return True

    if inspect.isfunction(dependency) and (dependency in dependency_graph.get(obj).defined_functions or \
        dependency in dependency_graph.get(obj).imported_functions):
        return True
    
    if inspect.isclass(dependency) and (dependency in dependency_graph.get(obj).defined_classes or \
        dependency in dependency_graph.get(obj).imported_classes):
        return True

    # Recurse through nested modules
    for module in dependency_graph.get(obj).defined_modules:
        if module in skip_modules: continue
        if has_dependency(module, dependency, dependency_graph, skip_modules=skip_modules):
            return True
        
        skip_modules.add(module)

    for module in dependency_graph.get(obj).imported_modules:
        if module in skip_modules: continue
        if has_dependency(module, dependency, dependency_graph, skip_modules=skip_modules):
            return True

        skip_modules.add(module)
        
    return False

def mainify_dependencies(obj: (types.ModuleType | types.FunctionType | object),
    skip_modules: set = set()) -> None:
    """ Redefines the object definition in __main__.

    Parameters:
        obj (types.ModuleType | types.FunctionType | object): The object which definition
                should be redefined into __main__.
        skip_modules (set: types.ModuleType): Modules that should be skipped during the
                redefinition process and be imported into __main__ instead.

    Note:
        - <skip_modules> is defined differently from that in has_dependency function.
        - <skip_modules> should be provided in order to prevent excessive redefining of
                nested dependencies; redefines imported packages and their nested
                dependencies wholesale by default.
    """
    if obj.__module__ == "__main__": return

    mainified_modules = set()
    module = inspect.getmodule(obj)
    dependency_graph = graph_dependencies(module)

    def decompose_references(source_code: str, reference: str, absolute_name: str) -> str:
        source_code_chunks = source_code.split(reference)
        source_code = '//' + source_code_chunks[0]
        
        for code_chunk in source_code_chunks[1:]:
            source_code += (
                reference if code_chunk and (source_code[-1].isalnum() or code_chunk[0].isalnum())
                else absolute_name
            ) + code_chunk

        return source_code[2:]

    def _mainify_dependencies(module: types.ModuleType) -> None:
        # Redefine definitions within <module> in __main__\
        if builtin_or_stdlib(module): return
        mainified_modules.add(module)

        # Package mainification
        if dependency_graph.get(module).defined_modules:
            for defined_module in dependency_graph.get(module).defined_modules:
                if not defined_module in skip_modules and not defined_module in mainified_modules:
                    _mainify_dependencies(defined_module)
            
            return # No subsequent execution required

        import_code, source_code = [], []

        # Extract source code without imports from module
        for defined_function in dependency_graph.get(module).defined_functions:
            source_code.append(inspect.getsource(defined_function))

        for defined_classes in dependency_graph.get(module).defined_classes:
            source_code.append(inspect.getsource(defined_classes))

        source_code = '\n'.join(source_code)

        for imported_module, module_name in dependency_graph.get(module).imported_modules.items():
            if not imported_module in mainified_modules:
                mainify_import = builtin_or_stdlib(imported_module)

                for skip_module in skip_modules:
                    if mainify_import: break
                    if has_dependency(skip_module, imported_module, dependency_graph):
                        mainify_import = True

                if mainify_import:
                    import_code.append(f"import {imported_module.__name__} as {module_name}\n")
                    skip_modules.add(imported_module)
                else:
                    _mainify_dependencies(imported_module)
            
            if imported_module in mainified_modules:
                source_code = source_code.replace(f"{module_name}.", '')

        for imported_function, (function_name, parent_module) in dependency_graph.get(module) \
            .imported_functions.items():
            if not parent_module in mainified_modules:
                mainify_import = builtin_or_stdlib(parent_module)

                for skip_module in skip_modules:
                    if mainify_import: break
                    if has_dependency(skip_module, parent_module, dependency_graph):
                        mainify_import = True

                if mainify_import:
                    import_code.append(f"from {parent_module.__name__} import {imported_function.__name__} as {function_name}")
                    skip_modules.add(parent_module)
                else:
                    _mainify_dependencies(parent_module)
            
            if parent_module in mainified_modules:
                source_code = decompose_references(source_code, function_name, imported_function.__name__)
        
        for imported_class, (class_name, parent_module) in dependency_graph.get(module) \
            .imported_classes.items():
            if not parent_module in mainified_modules:
                mainify_import = builtin_or_stdlib(parent_module)

                for skip_module in skip_modules:
                    if mainify_import: break
                    if has_dependency(skip_module, parent_module, dependency_graph):
                        mainify_import = True

                if mainify_import:
                    import_code.append(f"from {parent_module.__name__} import {imported_class.__name__} as {class_name}")
                    skip_modules.add(parent_module)
                else:
                    _mainify_dependencies(parent_module)
            
            if parent_module in mainified_modules:
                source_code = decompose_references(source_code, class_name, imported_class.__name__)
            
        source_code = '\n'.join(import_code) + f"\n{source_code}"
        executable_code = compile(source_code, "<string>", "exec")
        exec(executable_code, __main__.__dict__)
    
    _mainify_dependencies(module)

if __name__ == "__main__":
    pass