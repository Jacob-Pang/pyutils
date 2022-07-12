import os
import pandas as pd
import pyutils

from collections.abc import Iterable
from pyutils.database.data_node import DataNode
from pyutils.database.artifact import CloudPickleFile
from pyutils.dependency_tracer import DependencyGraph

class DataBase (DataNode):
    @staticmethod
    def memory_file_name(data_node_id: str) -> str:
        return f"{data_node_id}_dbase_memory"

    @staticmethod
    def restore_database(data_node_id: str, 
        connection_dpath: str = os.getcwd()) -> DataNode:
        return CloudPickleFile(DataBase.memory_file_name(data_node_id), connection_dpath).read_data()

    def __init__(self, data_node_id: str, connection_dpath: str = os.getcwd(),
        description: str = None, parent_database: any = None, **field_kwargs) -> None:

        super().__init__(data_node_id, connection_dpath, description, parent_database, **field_kwargs)
        self.child_nodes = dict()
        self.add_memory_node()
        
    def add_memory_node(self) -> None:
        memory_node = CloudPickleFile(DataBase.memory_file_name(self.data_node_id),
                description="persistent database memory structure")

        self.add_connected_child_node(memory_node)

    def save_database_memory(self, *args, dependency_graph: DependencyGraph = DependencyGraph(),
        **kwargs) -> None:
        self.get_child_node(DataBase.memory_file_name(self.data_node_id)).save_data(
                self, *args, dependency_graph=dependency_graph, **kwargs)

    def autosave_database_memory(self) -> None:
        self.save_database_memory()

    def get_child_node(self, data_node_id: str, recursive: bool = True) -> any:
        if data_node_id in self.child_nodes:
            return self.child_nodes.get(data_node_id)

        if recursive: # Traverse child networks
            for child_node in self.child_nodes.values():
                if not isinstance(child_node, DataBase):
                    continue

                grand_child_node = child_node.get_child_node(data_node_id)
                if grand_child_node: return grand_child_node

        return None

    def get_child_nodes(self, recursive: bool = False) -> set:
        child_nodes = set()

        for child_node in self.child_nodes.values():
            child_nodes.add(child_node)

            if isinstance(child_node, DataBase) and recursive:
                child_nodes = child_nodes.union(child_node.get_child_nodes(recursive))

        return child_nodes

    def get_connected_child_dpath(self, relative_dpath: str) -> str:
        if not relative_dpath:
            return self.connection_dpath

        return os.path.join(self.connection_dpath, relative_dpath)

    def add_child_node(self, data_node: DataNode) -> None:
        # Does not enforce ownership between this database and the child_node
        if data_node.data_node_id in self.child_nodes or \
            data_node.data_node_id == self.data_node_id:
            raise Exception()

        self.child_nodes[data_node.data_node_id] = data_node
        self.autosave_database_memory()

    def add_connected_child_node(self, data_node: DataNode, relative_dpath: str = '') -> None:
        # Setup connection paths and ownership
        data_node.connection_dpath = self.get_connected_child_dpath(relative_dpath)
        data_node.parent_database = self

        self.add_child_node(data_node)

    def destroy_child_node(self, data_node_id: str, *args, **kwargs) -> None:
        self.child_nodes.get(data_node_id).destroy_node(*args, **kwargs)
        self.child_nodes.pop(data_node_id)

        self.autosave_database_memory()

    def destroy_node(self, *args, **kwargs) -> None:
        for child_node in self.child_nodes:
            child_node.destroy_node(*args, **kwargs)

        self.autosave_database_memory()

    def __str__(self) -> str:
        return "DATABASE"

    def get_catalog(self, lookup_fields: Iterable = []) -> pd.DataFrame:
        child_nodes = self.get_child_nodes(recursive=True)
        catalog = []

        for child_node in child_nodes:
            node_fields = [child_node.data_node_id, str(child_node),
                    child_node.description]

            for lookup_field in lookup_fields:
                node_fields.append(getattr(child_node, lookup_field)
                        if hasattr(child_node, lookup_field) else None)

            catalog.append(node_fields)

        return pd.DataFrame(catalog, columns=["ID", "category",
                "description", *lookup_fields])
        
if __name__ == "__main__":
    pass