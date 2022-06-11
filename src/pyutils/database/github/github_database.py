from github import AuthenticatedUser, Github, Repository

from .github_artifact import GitHubPickleFile
from .github_data_node import GitHubDataNode
from ..data_node import DataNode
from ..database import DataBase
from ...github_ops.common import get_authenticated_repository, get_repository, github_relative_path
from ...github_ops.read_ops import read_pickle

class GitHubDataBase (GitHubDataNode, DataBase):
    @staticmethod
    def restore_database(data_node_id: str, user_name: str, repository_name: str,
        connection_dpath: str = '', branch: str = "main") -> GitHubDataNode:
        from_remote_file_path = github_relative_path(f"{connection_dpath}/{DataBase.memory_file_name(data_node_id)}")
        return read_pickle(user_name, repository_name, from_remote_file_path, branch)

    def __init__(self, data_node_id: str, user_name: str, repository_name: str,
        branch: str = "main", authenticated_user: AuthenticatedUser = None,
        authenticated_repo: Repository = None, request_timeout: int = 15,
        connection_dpath: str = '', description: str = None, parent_database: any = None, 
        **field_kwargs) -> None:

        self.user_name = user_name
        self.repository_name = repository_name
        self.branch = branch
        self.authenticated_user = authenticated_user
        self.authenticated_repo = authenticated_repo
        self.repository = authenticated_repo
        self.request_timeout = request_timeout

        super().__init__(data_node_id, connection_dpath, description, parent_database,
                **field_kwargs)

    def add_memory_node(self) -> None:
        memory_node = GitHubPickleFile(DataBase.memory_file_name(self.data_node_id),
                description="persistent database memory structure")

        self.add_connected_child_node(memory_node)

    def get_user_name(self) -> str:
        return self.user_name
    
    def get_repo_name(self) -> str:
        return self.repository_name

    def get_branch(self) -> str:
        return self.branch

    def get_authenticated_user(self, access_token: str = None) -> AuthenticatedUser:
        if self.authenticated_user:
            return self.authenticated_user
        
        self.authenticated_user = Github(access_token, timeout=self.request_timeout).get_user()
        return self.authenticated_user

    def get_authenticated_repo(self, access_token: str = None) -> Repository:
        if self.authenticated_repo:
            return self.authenticated_repo
        
        self.authenticated_repo = get_authenticated_repository(self.get_repo_name(),
                self.get_authenticated_user(access_token))
        
        self.repository = self.authenticated_repo
        return self.authenticated_repo

    def get_repository(self) -> Repository:
        if self.repository:
            return self.repository
        
        if self.authenticated_user:
            return self.get_authenticated_repo()
        
        self.repository = get_repository(self.get_repo_name(), self.get_user_name())
        return self.repository

    def save_database_memory(self, *args, access_token: str = None, commit_message: str = '',
        save_child_nodes: bool = True, **kwargs) -> None:
        authenticated_repo = self.get_authenticated_repo(access_token)
        # Removes cached authenticated github user and repositories
        self.authenticated_user = None
        self.authenticated_repo = None
        self.repository = None

        DataBase.save_database_memory(self, *args, authenticated_repo=authenticated_repo,
                access_token=access_token, commit_message=commit_message, **kwargs)
        
        if not save_child_nodes: return

        for child_node in self.child_nodes.values():
            if isinstance(child_node, GitHubDataBase):
                child_node.save_database_memory(*args, access_token=access_token,
                        commit_message=commit_message, save_child_nodes=True, **kwargs)

    def autosave_database_memory(self) -> None:
        pass # Does not perform auto save

    def add_connected_child_node(self, data_node: DataNode, relative_dpath: str = '') -> None:
        if isinstance(data_node, GitHubDataBase):
            data_node.user_name = self.user_name
            data_node.repository_name = self.repository_name
            data_node.branch = self.branch
            data_node.authenticated_user = self.authenticated_user
            data_node.authenticated_repo = self.authenticated_repo
            data_node.repository = self.repository

        DataBase.add_connected_child_node(self, data_node, relative_dpath)

if __name__ == "__main__":
    pass
