from pyutils.database.artifact import Artifact
from pyutils.database.data_node import DataNode

class DataFrameQuery (DataNode):
    def __init__(self, data_node_id: str, connection_dpath: str = None, description: str = None,
        host_database: any = None, **field_kwargs) -> None:
        super().__init__(data_node_id, connection_dpath, description, host_database, **field_kwargs)
        self.child_nodes = dict()

    def get_node_path(self) -> str:
        return None

    def destroy_node(self, **kwargs) -> None:
        return

    def read_data(self, **kwargs) -> any:
        raise NotImplementedError()

    def add_child_node(self, data_node: DataNode, **kwargs) -> None:
        # Overrides any existing node connected by the node ID.
        self.child_nodes[data_node.data_node_id] = data_node

    def __str__(self) -> str:
        return "QUERY"

class HashKeyQuery (DataFrameQuery):
    def add_child_node(self, data_node: Artifact, node_key: any = None, **kwargs) -> None:
        if not node_key:
            node_key = data_node.data_node_id

        self.child_nodes[node_key] = data_node

    def read_data(self, node_key: any, **kwargs) -> any:
        return self.child_nodes.get(node_key).read_data(**kwargs)

if __name__ == "__main__":
    pass
