import pickle

from github import Repository
from .github_data_node import GitHubDataNode
from ..artifact import Artifact, PickleFile
from ...github_ops.write_ops import write_files
from ...github_ops.read_ops import read_file, read_pickle

class GitHubArtifact (GitHubDataNode, Artifact):
    def make_connection_dpath(self) -> None:
        pass # Ignore operation

    def save_data_to_path(self, artifact_data: any, path: str, *args,
        authenticated_repo: Repository = None, access_token: str = None,
        commit_message: str = '', **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo(access_token)

        write_files(authenticated_repo, [artifact_data], [path],
                self.get_branch(), commit_message)

    def read_data_from_path(self, path: str, *args, **kwargs) -> any:
        return read_file(self.get_user_name(), self.get_repo_name(), path,
                self.get_branch())

class GitHubPickleFile (GitHubArtifact, PickleFile):
    def save_data_to_path(self, artifact_data: any, path: str, *args,
        authenticated_repo: Repository = None, access_token: str = None,
        commit_message: str = '', **kwargs) -> None:
        file_content = pickle.dumps(artifact_data)
        GitHubArtifact.save_data_to_path(self, file_content, path, *args,
                authenticated_repo=authenticated_repo, access_token=access_token,
                commit_message=commit_message, **kwargs)

    def read_data_from_path(self, path: str, *args, **kwargs) -> any:
        return read_pickle(self.get_user_name(), self.get_repo_name(), path,
                self.get_branch())

if __name__ == "__main__":
    pass
