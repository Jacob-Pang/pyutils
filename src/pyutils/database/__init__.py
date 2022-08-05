import cloudpickle
import os
import pandas as pd

from collections.abc import Iterable
from pyutils.database.data_node import DataNode
from pyutils.database.artifact import PickleFile

class DataBase (DataNode):
    @staticmethod
    def memory_file_name(data_node_id: str) -> str:
        return f"{data_node_id}_dbm"

    @staticmethod
    def restore_database(data_node_id: str, connection_dpath: str = os.getcwd()) -> DataNode:
        database = PickleFile(DataBase.memory_file_name(data_node_id), connection_dpath).read_data()

        for child_data_node_id, child_node in database.child_nodes.items():
            if not isinstance(child_node, DataBase): continue

            # Lazy update of child databases
            child_database = DataBase.restore_database(child_data_node_id, child_node.connection_dpath)

            if database.has_resident(child_database):
                child_database.host_database = database

            database.child_nodes[child_data_node_id] = child_database
        
        return database

    def __init__(self, data_node_id: str, connection_dpath: str = os.getcwd(), description: str = None,
        host_database: any = None, **field_kwargs) -> None:
        super().__init__(data_node_id, connection_dpath, description, host_database, **field_kwargs)
        self.child_nodes = dict()
        self.memory_file_node = self.init_memory_file_node()
        self.resync_memory_file_node()
    
    def init_memory_file_node(self) -> DataNode:
        return PickleFile(DataBase.memory_file_name(self.data_node_id), description=
                "persistent database memory structure")

    def resync_memory_file_node(self) -> None:
        self.memory_file_node.connection_dpath = self.connection_dpath
        self.memory_file_node.host_database = self
        
    def save_database_memory(self, **kwargs) -> None:
        self.resync_memory_file_node()
        self.memory_file_node.save_data(self, pickle_dump_fn=cloudpickle.dump, **kwargs)

    def autosave_database_memory(self) -> None:
        self.save_database_memory()

    def has_resident(self, data_node: DataNode) -> bool:
        return data_node.host_database and data_node.host_database.data_node_id == self.data_node_id

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

    def get_child_nodes(self, resident_nodes_only: bool = False, recursive: bool = False) -> set:
        child_nodes = set()

        for child_node in self.child_nodes.values():
            if resident_nodes_only and not self.has_resident(child_node):
                continue

            child_nodes.add(child_node)

            if isinstance(child_node, DataBase) and recursive:
                child_nodes = child_nodes.union(child_node.get_child_nodes(recursive))

        return child_nodes

    def get_resident_child_dpath(self, relative_dpath: str) -> str:
        if not relative_dpath:
            return self.connection_dpath

        return os.path.join(self.connection_dpath, relative_dpath)

    def add_child_node(self, data_node: DataNode) -> None:
        # Does not enforce ownership between this database and the child_node
        # Overrides any existing node connected by the node ID.
        self.child_nodes[data_node.data_node_id] = data_node
        self.autosave_database_memory()

    def add_resident_child_node(self, data_node: DataNode, relative_dpath: str = '') -> None:
        data_node.connection_dpath = self.get_resident_child_dpath(relative_dpath)
        data_node.host_database = self
        self.add_child_node(data_node)

    def destroy_child_node(self, data_node_id: str, **kwargs) -> None:
        self.child_nodes.get(data_node_id).destroy_node(**kwargs)
        self.child_nodes.pop(data_node_id)

        self.autosave_database_memory()

    def destroy_node(self, resident_nodes_only: bool = False, **kwargs) -> None:
        for child_node in self.get_child_nodes(resident_nodes_only, recursive=False):
            child_node.destroy_node(**kwargs)

        self.autosave_database_memory()

    def __str__(self) -> str:
        return "DATABASE"

    def get_catalog(self, lookup_fields: Iterable = []) -> pd.DataFrame:
        child_nodes = self.get_child_nodes(recursive=True)
        catalog = []

        for child_node in child_nodes:
            node_fields = [child_node.data_node_id, str(child_node), child_node.description]

            for lookup_field in lookup_fields:
                node_fields.append(getattr(child_node, lookup_field)
                        if hasattr(child_node, lookup_field) else None)

            catalog.append(node_fields)

        return pd.DataFrame(catalog, columns=["ID", "category", "description", *lookup_fields])
        
if __name__ == "__main__":
    pass