import os
import pickle
import time

from pyutils.database.data_node import DataNode
from pyutils import WrappedFunction

class Artifact (DataNode):
    def make_connection_dpath(self) -> None:
        if not os.path.exists(self.connection_dpath):
            os.makedirs(self.connection_dpath)

    def save_data(self, artifact_data: any, **kwargs) -> None:
        self.destroy_node(**kwargs)
        self.make_connection_dpath()
        self.save_data_to_path(artifact_data, self.get_node_path(), **kwargs)
        self.version_timestamp = time.time()

    def save_data_to_path(self, artifact_data: any, path: str) -> None:
        with open(path, 'w') as data_file:
            data_file.write(artifact_data)

    def read_data(self, **kwargs) -> any:
        return self.read_data_from_path(self.get_node_path(), **kwargs)

    def read_data_from_path(self, path: str, **kwargs):
        with open(path, 'r') as data_file:
            return data_file.read()

    def update_data(self, artifact_data: any, **kwargs) -> None:
        self.save_data(artifact_data, **kwargs)

    def __str__(self) -> str:
        return "ARTIFACT"

class PickleFile (Artifact):
    def save_data_to_path(self, artifact_data: any, path: str, pickle_dump_fn: callable = pickle.dump, **kwargs) -> None:
        kwargs = WrappedFunction.get_compat_kwargs(pickle_dump_fn, **kwargs)
        
        with open(path, 'wb') as data_file:
            pickle_dump_fn(artifact_data, data_file, protocol=pickle.HIGHEST_PROTOCOL, **kwargs)

    def read_data_from_path(self, path: str, pickle_load_fn: callable = pickle.load, **kwargs) -> any:
        kwargs = WrappedFunction.get_compat_kwargs(pickle_load_fn, **kwargs)

        with open(path, 'rb') as data_file:
            return pickle_load_fn(data_file, **kwargs)

if __name__ == "__main__":
    pass
