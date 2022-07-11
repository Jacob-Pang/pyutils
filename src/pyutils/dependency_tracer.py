import __main__
import ast
import importlib
import inspect
import os
import pkgutil
import re
import sys
import types

from collections.abc import Iterable

def builtin(reference: (types.ModuleType | types.FunctionType | type)) -> bool:
    # Returns whether the reference object is built-in
    reference = inspect.getmodule(reference)

    return not hasattr(reference, "__file__") or reference.__name__ in sys.builtin_module_names \
            or reference is None

def builtin_or_stdlib(reference: (types.ModuleType | types.FunctionType | type)) -> bool:
    # Returns whether the reference object is built-in or defined wihtin standard libraries.
    reference = inspect.getmodule(reference)

    return builtin(reference) or reference.__name__ in sys.stdlib_module_names

def unpack_packages(package: types.ModuleType, unpacked_modules: set = set(), ignore_uninstalled: bool = True) -> set:
    """ Unpacks only defined modules in <package>: does not check for module imports.
    """
    unpacked_modules.add(package)

    if hasattr(package, "__path__"):
        for _, module_name, is_package in pkgutil.iter_modules(package.__path__):
            if module_name == "__main__":
                continue

            try: # Nested dependencies may be un-installed
                unpacked_module = importlib.import_module(f".{module_name}", package=package.__name__)
            except Exception as uninstalled_exception:
                if ignore_uninstalled: continue
                raise uninstalled_exception

            unpacked_modules.add(unpacked_module)

            if is_package: # Recursively unpack modules
                unpack_packages(unpacked_module, unpacked_modules, ignore_uninstalled)

    return unpacked_modules

def get_reduced_source_code(module: types.ModuleType) -> str:
    source_code = inspect.getsource(module)
    current_package_name = module.__name__ if os.path.basename(module.__file__) == "__init__.py" \
                else '.'.join(module.__name__.split('.')[:-1])

    reduced_source_code_chunks = dict()

    def dump_source_code(node: ast.AST) -> None:
        if isinstance(node, ast.ImportFrom):
            source_code_chunk = ast.get_source_segment(source_code, node)
            parent_module_name = node.module

            if not parent_module_name:
                parent_module_name = current_package_name
            elif f".{parent_module_name} " in source_code_chunk:
                parent_module_name = f"{current_package_name}.{parent_module_name}"

            reduced_source_code_chunks[source_code_chunk] = f"\n{node.col_offset * ' '}".join([
                f"from {parent_module_name} import {relative_import.name}" + 
                (f" as {relative_import.asname}" if relative_import.asname else '')
                for relative_import in node.names
            ])
        elif isinstance(node, ast.Import):
            source_code_chunk = ast.get_source_segment(source_code, node)

            reduced_source_code_chunks[source_code_chunk] = f"\n{node.col_offset * ' '}".join([
                f"import {direct_import.name}" +
                (f" as {direct_import.asname}" if direct_import.asname else '')
                for direct_import in node.names
            ])
        elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.For, ast.If, ast.Try,
            ast.With, ast.While)):
            for child_node in ast.iter_child_nodes(node):
                dump_source_code(child_node)

    for child_node in ast.iter_child_nodes(ast.parse(source_code)):
        dump_source_code(child_node)

    for source_code_chunk, reduced_source_code_chunk in reduced_source_code_chunks.items():
        source_code = source_code.replace(source_code_chunk, reduced_source_code_chunk)

    return source_code

def remove_source_imports(source_code: str) -> str:
    return '\n'.join([
        source_code_chunk for source_code_chunk in source_code.split('\n')
        if not re.search("[^0-9a-zA-Z.]import[^0-9a-zA-Z]", source_code_chunk)
    ])

def remove_module_references(source_code: str, asname: str):
    # Remove references to <module_name> from the source code  
    for reference in re.findall(f"[^0-9a-zA-Z.]*{asname}.[0-9a-zA-Z]*[^0-9a-zA-Z.]",
        remove_source_imports(source_code), re.MULTILINE):
        source_code = source_code.replace(reference, reference[0] + reference[1:].replace(f"{asname}.", ''))

    return source_code

def decompose_references(source_code: str, asname: str, name: str) -> str:
    # Remove non-import references to <asname>
    for reference in re.findall(f"[^0-9a-zA-Z.]*{asname}[^0-9a-zA-Z]",
        remove_source_imports(source_code), re.MULTILINE):
        source_code = source_code.replace(reference, name.join(reference.split(asname)))

    return source_code

class DependencyGraph:
    def __init__(self, terminal_modules: set = set()) -> None:
        self.dependency_nodes = dict()
        self.terminal_modules = terminal_modules

    def set_terminal_module(self, terminal_module: types.ModuleType) -> None:
        """ Sets terminal status on the module, recursively imported dependencies and
        defined modules within the parent package.
        """
        if terminal_module in self.terminal_modules: return
        self.terminal_modules.add(terminal_module)

        try: # Ignore exceptions raised from inspect
            for _, imported_module in inspect.getmembers(terminal_module, inspect.ismodule):
                self.set_terminal_module(imported_module)
        except:
            pass

        package_name = terminal_module.__name__.split('.')[0]
        package = importlib.import_module(package_name)

        if package in self.terminal_modules: return
        self.terminal_modules.add(package)

        for defined_module in unpack_packages(package, ignore_uninstalled=True):
            self.set_terminal_module(defined_module)

class DependencyGraphNode:
    def __init__(self, module: types.ModuleType, dependency_graph: DependencyGraph = DependencyGraph()) -> None:
        # Set connection to graph
        self.module = module
        self.dependency_graph = dependency_graph
        self.dependency_graph.dependency_nodes[module] = self
        self.dependency_imports = dict()
        
    def branch_dependencies(self, ignore_uninstalled: bool = False) -> None:
        # Branching prerequisites
        if self.module in self.dependency_graph.terminal_modules:
            return

        if builtin_or_stdlib(self.module):
            self.dependency_graph.set_terminal_module(self.module)
            return

        try: # Does not support compiled module code
            self.reduced_source_code = get_reduced_source_code(self.module)
        except:
            self.reduced_source_code = None
            self.dependency_graph.set_terminal_module(self.module)
            return

        current_package_name = self.module.__name__ if os.path.basename(self.module.__file__) == "__init__.py" \
                else '.'.join(self.module.__name__.split('.')[:-1])

        def trace_dependency_imports(node: ast.AST, ignore_uninstalled: bool = False) -> None:
            try: # Wrap parsing process
                if isinstance(node, ast.ImportFrom):
                    source_code_chunk = ast.get_source_segment(self.reduced_source_code, node)
                    parent_module_name = node.module
                    if parent_module_name == "__main__": return

                    # Relative package imports
                    if not parent_module_name:
                        parent_module_name = current_package_name
                    elif f".{parent_module_name} " in source_code_chunk:
                        parent_module_name = f"{current_package_name}.{parent_module_name}"

                    parent_module = importlib.import_module(parent_module_name)
                    reference = node.names[0] # Reduced source ensures no grouped imports
                    asname = reference.asname if reference.asname else reference.name

                    # Imported class or function
                    if hasattr(parent_module, reference.name):
                        self.dependency_imports[asname] = (getattr(parent_module, reference.name),
                                source_code_chunk)
                        
                        if parent_module not in self.dependency_graph.dependency_nodes:
                            DependencyGraphNode(parent_module, self.dependency_graph) \
                                    .branch_dependencies(ignore_uninstalled)
                    
                    # Imported classes and functions
                    elif reference.name == "*": 
                        for class_name, class_ in inspect.getmembers(parent_module, inspect.isclass):
                            self.dependency_imports[class_name] = (class_, source_code_chunk)
                        
                        for function_name, function_ in inspect.getmembers(parent_module, inspect.isfunction):
                            self.dependency_imports[function_name] = (function_, source_code_chunk)

                        if parent_module not in self.dependency_graph.dependency_nodes:
                            DependencyGraphNode(parent_module, self.dependency_graph) \
                                    .branch_dependencies(ignore_uninstalled)
                    
                    # Imported module
                    else:
                        imported_module = importlib.import_module(f"{parent_module_name}.{reference.name}")
                        self.dependency_imports[asname] = (imported_module, source_code_chunk)

                        if imported_module not in self.dependency_graph.dependency_nodes:
                            DependencyGraphNode(imported_module, self.dependency_graph) \
                                    .branch_dependencies(ignore_uninstalled)

                elif isinstance(node, ast.Import):
                    source_code_chunk = ast.get_source_segment(self.reduced_source_code, node)
                    reference = node.names[0]
                    asname = reference.asname if reference.asname else reference.name

                    if reference.name == "__main__": return
                    imported_module = importlib.import_module(reference.name)
                    self.dependency_imports[asname] = (imported_module, source_code_chunk)

                    if imported_module not in self.dependency_graph.dependency_nodes:
                        DependencyGraphNode(imported_module, self.dependency_graph) \
                                .branch_dependencies(ignore_uninstalled)

                elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.For, ast.If, ast.Try,
                    ast.With, ast.While)): # Container descendant nodes

                    for child_node in ast.iter_child_nodes(node):
                        # Ignore exceptions for non-guaranteed execution
                        trace_dependency_imports(child_node, ignore_uninstalled=ignore_uninstalled or
                                isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.Try, ast.If)))
            except Exception as parse_exception:
                if not ignore_uninstalled: raise parse_exception

        for child_node in ast.iter_child_nodes(ast.parse(self.reduced_source_code)):
            trace_dependency_imports(child_node, ignore_uninstalled)

    def get_source_code(self, unpacked_dependencies: set = set()) -> (str | None):
        if self.module in self.dependency_graph.terminal_modules or builtin_or_stdlib(self.module):
            return None

        source_code = self.reduced_source_code
        unpacked_dependencies.add(self.module)
        source_code_chunks = []

        for asname, (dependency_import, source_code_chunk) in self.dependency_imports.items():
            module_import = dependency_import if inspect.ismodule(dependency_import) \
                    else inspect.getmodule(dependency_import)

            # Recursive codification of source.
            if module_import not in unpacked_dependencies and module_import in self.dependency_graph.dependency_nodes:
                dependency_source_code = self.dependency_graph.dependency_nodes.get(module_import) \
                        .get_source_code(unpacked_dependencies)

                if dependency_source_code:
                    source_code_chunks.append(dependency_source_code)
            
            if module_import in unpacked_dependencies: # Source was codified successfully.
                try:
                    source_code = remove_module_references(source_code, asname) \
                        if inspect.ismodule(dependency_import) else \
                        decompose_references(source_code, asname, dependency_import.__name__)
                except:
                    print(dependency_import)
                    raise Exception
                source_code = source_code.replace(source_code_chunk, '')
        
        # Append comment header
        source_code = f"# {self.module.__name__} unpacked source code SOF -------------------------\n{source_code}"
        source_code_chunks.append(source_code)
        source_code = '\n'.join(source_code_chunks)
        source_code = f"\n{source_code}# {self.module.__name__} unpacked source code EOF -------------------------"

        return source_code

def mainify_dependencies(obj: (types.ModuleType | types.FunctionType | object),
    dependency_graph: DependencyGraph = DependencyGraph(), unpacked_dependencies: set = set()) -> None:
    """
    Constraints (caa. 11 July 2022):
    a. Mainified modules must not have circular dependencies (order of execution cannot be resolved)
    b. Cannot mainify modules with binary source code (cython, executables)
    c. Cannot mainify from within the package (__main__ cannot be within the package directory)
    """
    # Redefines the object definition in __main__.
    if not hasattr(obj, "__module__") or obj.__module__ == "__main__":
        return

    module = inspect.getmodule(obj)
    if module in unpacked_dependencies: return

    node = DependencyGraphNode(module, dependency_graph)
    node.branch_dependencies()
    executable_code = compile(node.get_source_code(unpacked_dependencies), "<string>", "exec")
    exec(executable_code, __main__.__dict__)
    unpacked_dependencies.add(module)

    # Mainify nested attributes within collections.
    for name, attr in obj.__dict__.items():
        if name[0] == '_': continue # Skips private fields
        mainify_dependencies(attr, dependency_graph, unpacked_dependencies)

        if isinstance(attr, dict): # Mainify keys and values
            for key, value in attr.items():
                mainify_dependencies(key, dependency_graph, unpacked_dependencies)
                mainify_dependencies(value, dependency_graph, unpacked_dependencies)
        
        if isinstance(attr, Iterable):
            for nested_obj in attr:
                mainify_dependencies(nested_obj, dependency_graph, unpacked_dependencies)

if __name__ == "__main__":
    pass