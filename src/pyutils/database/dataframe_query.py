from pyutils.database.artifact import Artifact
from pyutils.database.data_node import DataNode

class DataFrameQuery (DataNode):
    def __init__(self, data_node_id: str, connection_dpath: str = None, description: str = None,
        parent_database: any = None, **field_kwargs) -> None:
        super().__init__(data_node_id, connection_dpath, description, parent_database, **field_kwargs)
        self.child_nodes = dict()

    def get_node_path(self) -> str:
        return None

    def destroy_node(self, *args, **kwargs) -> None:
        return

    def read_data(self, *args, **kwargs) -> any:
        raise NotImplementedError()

    def add_child_node(self, data_node: DataNode, *args, **kwargs) -> None:
        # Overrides any existing node connected by the node ID.
        self.child_nodes[data_node.data_node_id] = data_node

    def __str__(self) -> str:
        return "QUERY"

class HashKeyQuery (DataFrameQuery):
    def __init__(self, data_node_id: str, connection_dpath: str = None, description: str = None,
        parent_database: any = None, **field_kwargs) -> None:
        super().__init__(data_node_id, connection_dpath, description, parent_database, **field_kwargs)

    def add_child_node(self, data_node: Artifact, node_key: any = None, *args, **kwargs) -> None:
        if not node_key:
            node_key = data_node.data_node_id

        self.child_nodes[node_key] = data_node

    def read_data(self, node_key: any, *args, **kwargs) -> any:
        return self.child_nodes.get(node_key).read_data(*args, **kwargs)

if __name__ == "__main__":
    pass
