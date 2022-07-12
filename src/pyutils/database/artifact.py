import cloudpickle
import os
import pickle
import pyutils
import time

from pyutils.database.data_node import DataNode
from pyutils.dependency_tracer import DependencyGraph
from pyutils.dependency_tracer import mainify_dependencies
from pyutils.wrappers import RedirectIOStream

class Artifact (DataNode):
    def make_connection_dpath(self) -> None:
        if not os.path.exists(self.connection_dpath):
            os.makedirs(self.connection_dpath)

    def save_data(self, artifact_data: any, *args, **kwargs) -> None:
        self.destroy_node(*args, **kwargs)
        self.make_connection_dpath()
        self.save_data_to_path(artifact_data, self.get_node_path(), *args, **kwargs)
        self.version_timestamp = time.time()

    def save_data_to_path(self, artifact_data: any, path: str, *args, **kwargs) -> None:
        with open(path, 'w') as data_file:
            data_file.write(artifact_data)

    def read_data(self, *args, **kwargs) -> any:
        return self.read_data_from_path(self.get_node_path(), *args, **kwargs)

    def read_data_from_path(self, path: str, *args, **kwargs):
        with open(path, 'r') as data_file:
            return data_file.read()

    def update_data(self, artifact_data: any, *args, **kwargs) -> None:
        self.save_data(artifact_data, *args, **kwargs)

    def __str__(self) -> str:
        return "ARTIFACT"

class PickleFile (Artifact):
    def save_data_to_path(self, artifact_data: any, path: str, *args, **kwargs) -> None:
        with open(path, 'wb') as data_file:
            pickle.dump(artifact_data, data_file, protocol=pickle.HIGHEST_PROTOCOL)

    def read_data_from_path(self, path: str, *args, **kwargs) -> any:
        with open(path, 'rb') as data_file:
            return pickle.load(data_file)

class CloudPickleFile (Artifact):
    def save_data_to_path(self, artifact_data: any, path: str, *args, dependency_graph: DependencyGraph
        = DependencyGraph(), **kwargs) -> None:

        with RedirectIOStream(stdout_dest=os.devnull, stderr_dest=os.devnull):
            dependency_graph.set_terminal_module(pyutils)
            mainify_dependencies(self, dependency_graph)

        with open(path, 'wb') as data_file:
            cloudpickle.dump(artifact_data, data_file, protocol=pickle.HIGHEST_PROTOCOL)

    def read_data_from_path(self, path: str, *args, **kwargs) -> any:
        with open(path, 'rb') as data_file:
            return cloudpickle.load(data_file)

if __name__ == "__main__":
    pass
