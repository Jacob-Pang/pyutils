from github import AuthenticatedUser, Repository
from pyutils.database.data_node import DataNode
from pyutils.github_ops.common import github_relative_path
from pyutils.github_ops.write_ops import delete_file

class GitHubDataNode (DataNode):
    def __init__(self, data_node_id: str, connection_dpath: str = '', description: str = None,
        parent_database: any = None, **field_kwargs) -> None:
        super().__init__(data_node_id, connection_dpath, description,
                parent_database, **field_kwargs)

    def get_user_name(self) -> str:
        return self.parent_database.get_user_name()

    def get_repo_name(self) -> str:
        return self.parent_database.get_repo_name()

    def get_branch(self) -> str:
        return self.parent_database.get_branch()

    def get_authenticated_user(self, access_token: str = None) -> AuthenticatedUser:
        return self.parent_database.get_authenticated_user(access_token)

    def get_authenticated_repo(self, access_token: str = None) -> Repository:
        return self.parent_database.get_authenticated_repo(access_token)

    def get_repository(self, access_token: str = None) -> Repository:
        return self.parent_database.get_repository(access_token=access_token)

    def get_node_path(self) -> str:
        return github_relative_path(DataNode.get_node_path(self))

    def destroy_node(self, *args, authenticated_repo: Repository = None,
        access_token: str = None, commit_message: str = '', **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo(access_token)
        
        try:
            delete_file(authenticated_repo, self.get_node_path(),
                    self.get_branch(), commit_message=commit_message)
        except: pass # No files exist at remote directory

if __name__ == "__main__":
    pass
