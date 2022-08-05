from github import AuthenticatedUser, Repository
from pyutils.database.data_node import DataNode
from pyutils.github_ops import file_path_exists, github_relative_path
from pyutils.github_ops.write_ops import delete_file

class GitHubDataNode (DataNode):
    def __init__(self, data_node_id: str, connection_dpath: str = '', description: str = None,
        host_database: any = None, **field_kwargs) -> None:

        super().__init__(data_node_id, connection_dpath, description, **field_kwargs)
        self.host_database = host_database

    def set_access_token(self, access_token: str) -> None:
        self.host_database.set_access_token(access_token)

    def get_user_name(self) -> str:
        return self.host_database.get_user_name()

    def get_repo_name(self) -> str:
        return self.host_database.get_repo_name()

    def get_branch(self) -> str:
        return self.host_database.get_branch()

    def get_authenticated_user(self) -> AuthenticatedUser:
        return self.host_database.get_authenticated_user()

    def get_authenticated_repo(self) -> Repository:
        return self.host_database.get_authenticated_repo()

    def get_repository(self) -> Repository:
        return self.host_database.get_repository()

    def get_node_path(self) -> str:
        return github_relative_path(DataNode.get_node_path(self))

    def destroy_node(self, commit_message: str = '', authenticated_repo: Repository = None, **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo()

        if file_path_exists(authenticated_repo, self.get_branch(), self.get_branch(), use_github_api=True):
            delete_file(authenticated_repo, self.get_node_path(), self.get_branch(), commit_message)

if __name__ == "__main__":
    pass
