import pandas as pd

from github import Repository
from pyutils.database.dataframe import DataFrame, GraphDataFrame
from pyutils.database.github_database.github_artifact import GitHubArtifact
from pyutils.graph_dataframe import GraphDataFrameInterface, GraphDataFrameSchema
from pyutils.github_ops import address_exists, github_relative_path
from pyutils.github_ops.read_ops import read_csv_to_pandas, read_pickle
from pyutils.github_ops.write_ops import write_pandas_to_csv, write_pickle

class GitHubGraphDataFrameInterface (GraphDataFrameInterface):
    def __init__(self, user_name: str, repository_name: str, branch: str = "main",
        authenticated_repo: Repository = None, commit_message: str = '') -> None:
        self.user_name = user_name
        self.repository_name = repository_name
        self.branch = branch
        self.authenticated_repo = authenticated_repo
        self.commit_message = commit_message

    def set_commit_message(self, commit_message: str) -> None:
        self.commit_message = commit_message

    def file_path_exists(self, file_path: str) -> bool:
        return address_exists(github_relative_path(file_path))
    
    def save_schema_to_file_path(self, graph_schema: GraphDataFrameSchema, file_path: str) -> None:
        write_pickle(graph_schema, self.authenticated_repo, github_relative_path(file_path),
                self.branch, self.commit_message)

    def save_data_to_file_path(self, pdf: pd.DataFrame, file_path: str) -> None:
        write_pandas_to_csv(pdf, self.authenticated_repo, github_relative_path(file_path), self.branch,
                self.commit_message, index=True)

    def read_schema_from_file_path(self, file_path: str) -> GraphDataFrameSchema:
        return read_pickle(self.user_name, self.repository_name, github_relative_path(file_path), self.branch)

    def read_data_from_file_path(self, file_path: str) -> pd.DataFrame:
        return read_csv_to_pandas(self.user_name, self.repository_name, github_relative_path(file_path),
                self.branch, index_col=0)

class GitHubDataFrame (GitHubArtifact, DataFrame):
    def save_data_to_path(self, artifact_data: pd.DataFrame, path: str, commit_message: str = '',
        authenticated_repo: Repository = None, **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo()

        write_pandas_to_csv(artifact_data, authenticated_repo, path, self.get_branch(),
                commit_message, index=True)

    def read_data_from_path(self, path: str, **kwargs) -> any:
        return read_csv_to_pandas(self.get_user_name(), self.get_repo_name(), path,
                self.get_branch(), **kwargs)

class GitHubGraphDataFrame (GitHubArtifact, GraphDataFrame):
    def save_data_to_path(self, artifact_data: pd.DataFrame, path: str, partition_columns: list = list(),
        commit_message: str = '', authenticated_repo: Repository = None, **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo()

        GitHubGraphDataFrameInterface(self.get_user_name(), self.get_repo_name(), self.get_branch(),
                authenticated_repo, commit_message) \
                .save_dataframe(artifact_data, path, partition_columns)

    def read_data_from_path(self, path: str, query_predicates: set = set(), **kwargs) -> any:
        return GitHubGraphDataFrameInterface(self.get_user_name(), self.get_repo_name(), self.get_branch()) \
                .read_dataframe(path, query_predicates)

    def update_data(self, artifact_data: any, commit_message: str = '', authenticated_repo: Repository = None, **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo()
        
        GitHubGraphDataFrameInterface(self.get_user_name(), self.get_repo_name(), self.get_branch(),
                authenticated_repo, commit_message) \
                .merge_dataframe(artifact_data, self.get_node_path(), self.merge_function)

if __name__ == "__main__":
    pass
