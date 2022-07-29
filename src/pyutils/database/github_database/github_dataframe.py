import io
import os
import shutil
import time
import pandas as pd

from github import Repository
from pyutils.database.dataframe import DataFrame, ParquetDataFrame
from pyutils.database.github_database.github_artifact import GitHubArtifact
from pyutils.github_ops.common import github_relative_path, repository_walk
from pyutils.github_ops.write_ops import delete_file, push_directory, write_files
from pyutils.github_ops.read_ops import pull_directory, read_csv_to_pandas, read_parquet_to_pandas

class GitHubDataFrame (GitHubArtifact, DataFrame):
    def save_data_to_path(self, artifact_data: any, path: str, *args, authenticated_repo: Repository = None,
        access_token: str = None, commit_message: str = '', **kwargs) -> None:

        file_content = io.StringIO()
        artifact_data.to_csv(file_content, index=False)
        GitHubArtifact.save_data_to_path(self, file_content.getvalue(), path, *args, authenticated_repo=authenticated_repo,
                access_token=access_token, commit_message=commit_message, **kwargs)

    def read_data_from_path(self, path: str, *args, **kwargs) -> any:
        return read_csv_to_pandas(self.get_user_name(), self.get_repo_name(), path, self.get_branch(), **kwargs)

class GitHubParquetDataFrame (GitHubArtifact, ParquetDataFrame):
    def get_partition_path(self, partition_field_values: tuple) -> str:
        return github_relative_path(ParquetDataFrame.get_partition_path(self, partition_field_values))
    
    def save_data(self, artifact_data: any, *args, authenticated_repo: Repository = None,
        access_token: str = None, partition_columns: list = None, **kwargs) -> None:
        return ParquetDataFrame.save_data(self, artifact_data, *args, authenticated_repo=authenticated_repo,
                access_token=access_token, partition_columns=partition_columns, **kwargs)

    def save_data_to_path(self, artifact_data: any, path: str, *args, authenticated_repo: Repository = None,
        access_token: str = None, partition_cols: list = None, commit_message: str = '', **kwargs) -> None:
        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo(access_token)

        if not partition_cols: # Single parquet file: direct write
            file_content = io.BytesIO()
            artifact_data.to_parquet(file_content)
        
            return write_files(authenticated_repo, [file_content.getbuffer().tobytes()],
                    [path], branch=self.get_branch(), commit_message=commit_message)

        # Writen to local directory before pushing as parquet paritioning results in multiple files
        from_temp_local_directory_path = os.path.join(os.getcwd(),
                "github_parquet.save_data_cache")

        if not os.path.exists(from_temp_local_directory_path):
            os.makedirs(from_temp_local_directory_path)

        temp_local_file_path = os.path.join(from_temp_local_directory_path, self.data_node_id)
        ParquetDataFrame.save_data_to_path(self, artifact_data, temp_local_file_path,
                partition_cols=partition_cols)

        push_directory(authenticated_repo, from_temp_local_directory_path, os.path.dirname(path),
                self.get_branch(), commit_message)

        shutil.rmtree(from_temp_local_directory_path)
        self.version_timestamp = time.time()

    def read_data(self, *args, filters: list = None, access_token: str = None, **kwargs) -> pd.DataFrame:
        if not filters or not self.partition_columns:
            return ParquetDataFrame.read_data(self, *args, access_token=access_token, **kwargs)

        repository = self.get_authenticated_repo(access_token) if access_token else self.get_repository()
        remote_file_paths = [
            remote_file_obj.path for remote_file_obj in
            repository_walk(repository, self.get_node_path(), self.get_branch())
            if self.filter_partition_path(remote_file_obj.path, filters)
        ]

        temp_local_directory_path = os.path.join(os.getcwd(), "github_parquet.read_data_cache")
        temp_local_file_path = os.path.join(temp_local_directory_path, self.data_node_id)

        pull_directory(self.get_user_name(), self.get_repo_name(), self.get_node_path(),
                remote_file_paths, temp_local_file_path, self.get_branch(), repository)
            
        artifact_data = ParquetDataFrame.read_data_from_path(self, temp_local_file_path,
                filters=self.apply_schema_to_filters(filters))

        self.dataframe_schema.reverse_reduced_schema(artifact_data, inplace=True)
        shutil.rmtree(temp_local_directory_path)

        return artifact_data

    def read_data_from_path(self, path: str, *args, authenticated_repo: Repository = None,
        access_token: str = None, **kwargs) -> pd.DataFrame:
        if access_token and not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo(access_token)

        return read_parquet_to_pandas(self.get_user_name(), self.get_repo_name(), path,
                self.get_branch(), repository=authenticated_repo)

    def update_data(self, artifact_data: pd.DataFrame, *args, authenticated_repo: Repository = None,
        access_token: str = None, commit_message: str = '', **kwargs) -> None:
        if not self.partition_columns:
            return ParquetDataFrame.update_data(self, artifact_data, *args,
                    authenticated_repo=authenticated_repo, access_token=access_token,
                    commit_message=commit_message, **kwargs)

        if not authenticated_repo:
            authenticated_repo = self.get_authenticated_repo(access_token)

        self.dataframe_schema.apply_reduced_schema(artifact_data, inplace=True)
        temp_local_directory_path = os.path.join(os.getcwd(), "github_parquet.update_data_cache")
        temp_local_file_path = os.path.join(temp_local_directory_path, self.data_node_id)

        # Partitioning update optimization
        for partition_field_values, partition_artifact_data in artifact_data.groupby(
            by=self.partition_columns):
            partition_dpath = self.get_partition_path(partition_field_values)
            partition_file_path = None

            try: # Attempt to retrieve previous partition data
                remote_partition_contents = authenticated_repo.get_contents(partition_dpath)
                partition_file_path = remote_partition_contents[0].path
            except: pass

            print(partition_field_values, partition_file_path)

            if partition_file_path:
                previous_partition_artifact_data = self.read_data_from_path(partition_file_path,
                        *args, access_token=access_token, **kwargs)
                
                # Set field_values
                for field_value, field_name in zip(partition_field_values, self.partition_columns):
                    previous_partition_artifact_data[field_name] = field_value

                partition_artifact_data = pd.concat([partition_artifact_data,
                        previous_partition_artifact_data], axis=0)

                self.drop_duplicates(partition_artifact_data)
                delete_file(authenticated_repo, partition_file_path, self.get_branch(),
                        commit_message=commit_message)

            ParquetDataFrame.save_data_to_path(self, partition_artifact_data, temp_local_file_path,
                    partition_cols=self.partition_columns)

        push_directory(authenticated_repo, temp_local_file_path, self.get_node_path(), self.get_branch(),
                commit_message=commit_message)

        shutil.rmtree(temp_local_directory_path)

if __name__ == "__main__":
    pass
