import cloudpickle

from github import AuthenticatedUser, Github, Repository
from pyutils.database import DataBase
from pyutils.database.data_node import DataNode
from pyutils.database.github_database.github_artifact import GitHubPickleFile
from pyutils.database.github_database.github_data_node import GitHubDataNode
from pyutils.github_ops import get_authenticated_repository, get_repository, github_relative_path
from pyutils.github_ops.read_ops import read_pickle

class GitHubDataBase (GitHubDataNode, DataBase):
    class RequiresAuthenticationException (Exception):
        def __init__(self) -> None:
            super().__init__("Cannot proceed without authentication: use <set_access_token> method.")

    @staticmethod
    def restore_database(data_node_id: str, user_name: str, repository_name: str, connection_dpath: str = '',
        branch: str = "main", sync_connected_databases: bool = True, host_database: any = None,
        repository: Repository = None) -> GitHubDataNode:
        
        if not repository:
            repository = get_repository(repository_name, user_name)
        
        from_remote_file_path = github_relative_path(f"{connection_dpath}/{DataBase.memory_file_name(data_node_id)}")
        database = read_pickle(repository, from_remote_file_path, branch, pickle_loads_fn=cloudpickle.loads)

        if not sync_connected_databases:
            return database
        
        for child_data_node_id, child_node in database.child_nodes.items():
            # Lazy update of child databases
            if isinstance(child_node, GitHubDataBase):
                database.child_nodes[child_data_node_id] = GitHubDataBase.restore_database(
                    child_data_node_id, child_node.get_user_name(), child_node.get_repo_name(),
                    child_node.connection_dpath, child_node.get_branch(),
                    sync_connected_databases=True,
                    host_database=(database if database.has_resident(child_node) else None),
                    repository=(repository if database.has_resident(child_node) else None)
                )

        # Lazy update of host database
        if host_database:
            database.host_database = host_database
        elif database.host_database:
            host_database = GitHubDataBase.restore_database(
                database.host_database.data_node_id, database.host_database.get_user_name(),
                database.host_database.get_repo_name(), database.host_database.connection_dpath,
                database.host_database.get_branch(), sync_connected_databases=False,
                repository=repository)

            host_database.child_nodes[database.data_node_id] = database
            database.host_database = host_database
        
        return database

    def __init__(self, data_node_id: str, user_name: str = None, repository_name: str = None,
        branch: str = "main", access_token: str = None, authenticated_user: AuthenticatedUser = None,
        authenticated_repo: Repository = None, request_timeout: int = 15, connection_dpath: str = '',
        description: str = None, host_database: any = None, **field_kwargs) -> None:

        self.user_name = user_name
        self.repository_name = repository_name
        self.branch = branch
        self.access_token = access_token
        self.authenticated_user = authenticated_user
        self.authenticated_repo = authenticated_repo
        self.repository = authenticated_repo
        self.request_timeout = request_timeout

        super().__init__(data_node_id, connection_dpath, description, host_database, **field_kwargs)

    def init_memory_file_node(self) -> GitHubPickleFile:
        return GitHubPickleFile(DataBase.memory_file_name(self.data_node_id),
                description="persistent database memory structure")

    def set_access_token(self, access_token: str) -> None:
        if self.host_database:
            return self.host_database.set_access_token(access_token)

        self.access_token = access_token

    def get_user_name(self) -> str:
        if self.host_database:
            return self.host_database.get_user_name()

        return self.user_name
    
    def get_repo_name(self) -> str:
        if self.host_database:
            return self.host_database.get_repo_name()

        return self.repository_name

    def get_branch(self) -> str:
        if self.host_database:
            return self.host_database.get_branch()

        return self.branch

    def get_authenticated_user(self) -> AuthenticatedUser:
        if self.host_database:
            return self.host_database.get_authenticated_user()

        if self.authenticated_user:
            return self.authenticated_user
        
        if not self.access_token:
            raise GitHubDataBase.RequiresAuthenticationException()

        self.authenticated_user = Github(self.access_token, timeout=self.request_timeout).get_user()
        return self.authenticated_user

    def get_authenticated_repo(self) -> Repository:
        # Returns an authenticated repo
        if self.host_database:
            return self.host_database.get_authenticated_repo()

        if self.authenticated_repo:
            return self.authenticated_repo
        
        self.authenticated_repo = get_authenticated_repository(self.get_repo_name(),
                self.get_authenticated_user())
        
        self.repository = self.authenticated_repo
        return self.authenticated_repo

    def get_repository(self) -> Repository:
        # Returns an authenticated repo where possible
        if self.host_database:
            return self.host_database.get_repository()

        if self.access_token or self.authenticated_user:
            return self.get_authenticated_repo()

        if self.repository:
            return self.repository
        
        self.repository = get_repository(self.get_repo_name(), self.get_user_name())
        return self.repository

    def get_authentication_cache(self) -> dict:
        if self.host_database:
            return self.host_database.get_authentication_cache()

        return {
            "access_token": self.access_token,
            "authenticated_user": self.authenticated_user,
            "authenticated_repo": self.authenticated_repo,
            "repository": self.repository
        }

    def destroy_authentication_cache(self) -> None:
        if self.host_database:
            return self.host_database.destroy_authentication_cache()

        self.access_token = None
        self.authenticated_user = None
        self.authenticated_repo = None
        self.repository = None

    def set_authentication_cache(self, authentication_cache: dict) -> None:
        if self.host_database:
            return self.host_database.set_authentication_cache(authentication_cache)

        self.access_token = authentication_cache.get("access_token")
        self.authenticated_user = authentication_cache.get("authenticated_user")
        self.authenticated_repo = authentication_cache.get("authenticated_repo")
        self.repository = authentication_cache.get("repository")

    def save_database_memory(self, commit_message: str = '', authenticated_repo: Repository = None,
        save_child_nodes: bool = True, **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo()
        
        # Destroy authentication caches
        authentication_caches = dict()
        authentication_caches[self.data_node_id] = self.get_authentication_cache()
        self.destroy_authentication_cache()

        for child_node in self.child_nodes.values():
            if isinstance(child_node, GitHubDataBase):
                if save_child_nodes:
                    child_node.save_database_memory(commit_message=commit_message, save_child_nodes=True,
                            authenticated_repo=(authenticated_repo if self.has_resident(child_node) else None),
                            **kwargs)

                authentication_caches[child_node.data_node_id] = child_node.get_authentication_cache()
                child_node.destroy_authentication_cache()

        DataBase.save_database_memory(self, authenticated_repo=authenticated_repo, commit_message=
                commit_message, **kwargs)

        # Reset authentication caches
        self.set_authentication_cache(authentication_caches.get(self.data_node_id))

        for child_node in self.child_nodes.values():
            if isinstance(child_node, GitHubDataBase):
                child_node.set_authentication_cache(authentication_caches.get(child_node.data_node_id))

    def autosave_database_memory(self) -> None:
        pass # Does not perform auto save

    def add_resident_child_node(self, data_node: DataNode, relative_dpath: str = '') -> None:
        if isinstance(data_node, GitHubDataBase):
            data_node.destroy_authentication_cache()
            data_node.user_name = self.user_name
            data_node.repository_name = self.repository_name
            data_node.branch = self.branch

        return super().add_resident_child_node(data_node, relative_dpath)

if __name__ == "__main__":
    pass