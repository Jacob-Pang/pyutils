import cloudpickle
import pyutils

from github import AuthenticatedUser, Github, Repository
from pyutils.database import DataBase
from pyutils.database.data_node import DataNode
from pyutils.database.github_database.github_artifact import GitHubCloudPickleFile
from pyutils.database.github_database.github_data_node import GitHubDataNode
from pyutils.dependency_tracer import DependencyGraph
from pyutils.github_ops.common import get_authenticated_repository, get_repository, github_relative_path
from pyutils.github_ops.read_ops import read_file

class GitHubDataBase (GitHubDataNode, DataBase):
    @staticmethod
    def restore_database(data_node_id: str, user_name: str, repository_name: str,
        connection_dpath: str = '', branch: str = "main") -> GitHubDataNode:
        from_remote_file_path = github_relative_path(f"{connection_dpath}/{DataBase.memory_file_name(data_node_id)}")
        return cloudpickle.loads(read_file(user_name, repository_name, from_remote_file_path, branch))

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
        memory_node = GitHubCloudPickleFile(DataBase.memory_file_name(self.data_node_id),
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

    def destroy_authentication_cache(self) -> None:
        self.authenticated_user = None
        self.authenticated_repo = None
        self.repository = None

    def save_database_memory(self, *args, access_token: str = None, commit_message: str = '',
        save_child_nodes: bool = True, dependency_graph: DependencyGraph = DependencyGraph(pyutils),
        **kwargs) -> None:
        """
        Notes:
            method removes cached authenticated github objects before saving. User has to
                provide access token again to re-authenticate in subsequent writes.
        """
        for child_node in self.child_nodes.values():
            if isinstance(child_node, GitHubDataBase):
                if save_child_nodes:
                    child_node.save_database_memory(*args, access_token=access_token,
                            commit_message=commit_message, save_child_nodes=True,
                            dependency_graph=dependency_graph, **kwargs)
                
                child_node.destroy_authentication_cache()

        authenticated_repo = self.get_authenticated_repo(access_token)
        self.destroy_authentication_cache()

        DataBase.save_database_memory(self, *args, authenticated_repo=authenticated_repo,
                access_token=access_token, commit_message=commit_message,
                dependency_graph=dependency_graph, **kwargs)

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