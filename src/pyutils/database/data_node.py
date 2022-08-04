import os
import shutil

class DataNode:
    def __init__(self, data_node_id: str, connection_dpath: str = os.getcwd(), description: str = None,
        host_database: any = None, **field_kwargs) -> None:

        self.data_node_id = data_node_id
        self.connection_dpath = connection_dpath
        self.host_database = host_database
        self.description = description
        self.version_timestamp = None

        for field_name, field_value in field_kwargs.items():
            setattr(self, field_name, field_value)

    def __hash__(self) -> int:
        return self.data_node_id.__hash__()

    def get_node_path(self) -> str:
        return os.path.join(self.connection_dpath, self.data_node_id)

    def destroy_node(self, **kwargs) -> None:
        self.version_timestamp = None

        if not os.path.exists(self.get_node_path()):
            return

        if os.path.isdir(self.get_node_path()):
            shutil.rmtree(self.get_node_path())
        else: # Node assigned to file_path
            os.remove(self.get_node_path())

    def __str__(self) -> str:
        return "DATA_NODE"

if __name__ == "__main__":
    pass
