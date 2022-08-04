import pickle

from github import Repository
from pyutils.database.artifact import Artifact, PickleFile
from pyutils.database.github_database.github_data_node import GitHubDataNode
from pyutils.github_ops.write_ops import write_files, write_pickle
from pyutils.github_ops.read_ops import read_file, read_pickle

class GitHubArtifact (GitHubDataNode, Artifact):
    def make_connection_dpath(self) -> None:
        pass # Ignore operation

    def save_data_to_path(self, artifact_data: any, path: str, commit_message: str = '',
        authenticated_repo: Repository = None, **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo()

        write_files(authenticated_repo, [artifact_data], [path], self.get_branch(), commit_message)

    def read_data_from_path(self, path: str, **kwargs) -> any:
        return read_file(self.get_user_name(), self.get_repo_name(), path, self.get_branch())

class GitHubPickleFile (GitHubArtifact, PickleFile):
    def save_data_to_path(self, artifact_data: any, path: str, pickle_dumps_fn: callable = pickle.dumps,
        commit_message: str = '', authenticated_repo: Repository = None, **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo()

        return write_pickle(artifact_data, authenticated_repo, path, self.get_branch(), commit_message,
                pickle_dumps_fn)

    def read_data_from_path(self, path: str, pickle_loads_fn: callable = pickle.loads, **kwargs) -> any:
        return read_pickle(self.get_user_name(), self.get_repo_name(), path, self.get_branch(), pickle_loads_fn)

if __name__ == "__main__":
    pass
