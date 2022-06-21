import __main__
import importlib
import inspect
import os
import pkgutil
import sys
import types

from collections.abc import Iterable
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
        if not module or builtin_or_stdlib(module) or module in skip_modules or module in dependency_graph:
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
    def _has_dependency(module: types.ModuleType) -> bool:
        if builtin_or_stdlib(module) or module == __main__ or module in skip_modules:
            return False

        if not module in dependency_graph: # Unrecorded dependency
            graph_dependencies(module, dependency_graph)

        skip_modules.add(module)

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
        for module in {*dependency_graph.get(obj).defined_modules, *dependency_graph.get(obj).imported_modules}:
            if _has_dependency(module):
                return True
            
        return False

    if inspect.isfunction(obj) or inspect.isclass(obj):
        # Set the defining module as the subject of dependency
        return _has_dependency(inspect.getmodule(obj))
    
    return _has_dependency(obj)

def mainify_dependencies(obj: (types.ModuleType | types.FunctionType | object),
    dependency_graph: dict = dict(), skip_modules: set = set()) -> None:
    """ Redefines the object definition in __main__.

    Parameters:
        obj (types.ModuleType | types.FunctionType | object): The object which definition
                should be redefined into __main__.
        dependency_graph (dict, opt): The output from graph_dependencies.
        skip_modules (set: types.ModuleType, opt): Modules that should be skipped during the
                redefinition process and be imported into __main__ instead.

    Note:
        - <skip_modules> is defined differently from that in has_dependency function.
        - <skip_modules> should be provided in order to prevent excessive redefining of
                nested dependencies; redefines imported packages and their nested
                dependencies wholesale by default.
        - mainify_dependencies cannot redefine multiple classes, modules or functions with
                the same name.
    """
    if not hasattr(obj, "__module__") or obj.__module__ == "__main__":
        return

    mainified_modules = set()
    module = inspect.getmodule(obj)
    dependency_graph = graph_dependencies(module, dependency_graph)

    def decompose_references(source_code: str, reference: str, absolute_name: str) -> str:
        source_code_chunks = source_code.split(reference)
        source_code = '//' + source_code_chunks[0]
        
        for code_chunk in source_code_chunks[1:]:
            source_code += (
                reference if code_chunk and (source_code[-1].isalnum() or code_chunk[0].isalnum())
                else absolute_name
            ) + code_chunk

        return source_code[2:]

    def _mainify_dependencies(module: types.ModuleType) -> bool:
        # Redefine definitions within <module> in __main__\
        # Returns whether the module was mainified.
        if builtin_or_stdlib(module) or module in mainified_modules or module in skip_modules:
            return False

        for skip_module in skip_modules:
            if has_dependency(skip_module, module, dependency_graph):
                skip_modules.add(module)
                return False

        mainified_modules.add(module)

        # Package mainification
        if dependency_graph.get(module).defined_modules:
            for defined_module in dependency_graph.get(module).defined_modules:
                _mainify_dependencies(defined_module)
            
            return # No subsequent execution required

        source_code = inspect.getsource(module)
        import_code_chunks, source_code_chunks = [], []

        # Extract source code without imports from module while maintaining definition sequence
        for defined_function in dependency_graph.get(module).defined_functions:
            function_definition = inspect.getsource(defined_function)
            source_code_chunks.append((source_code.find(function_definition), function_definition))

        for defined_classes in dependency_graph.get(module).defined_classes:
            class_definition = inspect.getsource(defined_classes)
            source_code_chunks.append((source_code.find(class_definition), class_definition))

        source_code = '\n'.join([code_chunk for _, code_chunk in sorted(source_code_chunks)])

        for imported_module, module_name in dependency_graph.get(module).imported_modules.items():
            if _mainify_dependencies(imported_module):
                source_code = source_code.replace(f"{module_name}.", '')
            else: # Import in __main__
                import_code_chunks.append(f"import {imported_module.__name__} as {module_name}\n")
                
        for imported_function, (function_name, parent_module) in dependency_graph.get(module) \
            .imported_functions.items():
            if _mainify_dependencies(parent_module):
                source_code = decompose_references(source_code, function_name, imported_function.__name__)
            else: # Import in __main__
                import_code_chunks.append(f"from {parent_module.__name__} import {imported_function.__name__} " + \
                        f"as {function_name}")
        
        for imported_class, (class_name, parent_module) in dependency_graph.get(module) \
            .imported_classes.items():
            if _mainify_dependencies(parent_module):
                source_code = decompose_references(source_code, class_name, imported_class.__name__)
            else: # Import in __main__
                import_code_chunks.append(f"from {parent_module.__name__} import {imported_class.__name__} as {class_name}")
        
        try:
            source_code = '\n'.join(import_code_chunks) + f"\n{source_code}"
            print("***************************************************************************************************")
            print(module.__name__)
            print(source_code)
            executable_code = compile(source_code, "<string>", "exec")
            exec(executable_code, __main__.__dict__)
            return True
        except Exception as e:
            print(skip_modules)
            print("*******************************************************************")
            print(inspect.getsource(module))
            raise e
    
    _mainify_dependencies(module)

    # Mainify nested (undefined) attributes
    for name, attr in obj.__dict__.items():
        if name[0] == '_': continue # Skips private fields
        mainify_dependencies(attr, dependency_graph=dependency_graph, skip_modules=skip_modules)

        if isinstance(attr, dict): # Mainify keys and values
            for key, value in attr.items():
                mainify_dependencies(key, dependency_graph=dependency_graph, skip_modules=skip_modules)
                mainify_dependencies(value, dependency_graph=dependency_graph, skip_modules=skip_modules)
        
        if isinstance(attr, Iterable):
            for nested_obj in attr:
                mainify_dependencies(nested_obj, dependency_graph=dependency_graph, skip_modules=skip_modules)

if __name__ == "__main__":
    pass