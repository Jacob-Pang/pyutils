import io
import os
import pickle
import requests
import shutil
import pandas as pd

from github import Repository
from collections.abc import Iterable
from pyutils.github_ops import raw_github_address, repository_walk

def read_file(repository: Repository, from_remote_file_path: str, branch: str = "main") -> any:
    """ Reads the contents from a file in remote Github repository.

    Parameters:
        user_name (str): The Github user.
        repository_name (str): The Remote repository name.
        from_remote_file_path (str): The relative filepath within the remote repository.
        branch (str): The branch of the repository to pull from.

    Returns:
        file_content (any): The contents of the remote file in text or bytes.
    """
    return requests.get(raw_github_address(repository, from_remote_file_path, branch)).content

def pull_directory(repository: Repository, from_remote_directory_path: str = "",
    from_remote_file_paths: Iterable = None, to_local_directory_path: str = os.getcwd(),
    branch: str = "main") -> None:
    """ Pulls the contents of a remote Github repository directory.

    Parameters:
        repository (Repository, opt): pygithub.Repository object. Can be authenticated
                to reduce API requests.
        from_remote_directory_path (str): The relative remote directory path.
        from_remote_file_paths (Iterable): The files to pull from the directory. If
                not specified, pulls all files from the remote directory.
        to_local_directory_path (str): The local directory to write the contents into.
        branch (str): The branch of the repository to pull from.
    """
    if not from_remote_file_paths:
        from_remote_file_paths = [
            remote_file_obj.path for remote_file_obj in
            repository_walk(repository, from_remote_directory_path, branch)
        ]

    for from_remote_file_path in from_remote_file_paths:
        local_file_path = os.path.join(
            to_local_directory_path,
            from_remote_file_path.replace(from_remote_directory_path, '')
                    .strip('/').replace('/', os.path.sep)
        )

        if not os.path.exists(os.path.dirname(local_file_path)):
            os.makedirs(os.path.dirname(local_file_path))

        file_content = read_file(repository, from_remote_file_path, branch)

        with open(local_file_path, "wb") as file_output:
            file_output.write(file_content)

# Read operations for specific classes
def read_pickle(repository: Repository, from_remote_file_path: str, branch: str = "main",
    pickle_loads_fn: callable = pickle.loads) -> any:
    return pickle_loads_fn(read_file(repository, from_remote_file_path, branch))

def read_csv_to_pandas(repository: Repository, from_remote_file_path: str,
    branch: str = "main", **read_csv_kwargs) -> pd.DataFrame:
    # Wrapper around <read_remote_file> to read csv into pandas dataframes.
    return pd.read_csv(io.BytesIO(read_file(repository, from_remote_file_path, branch)), **read_csv_kwargs)

if __name__ == "__main__":
    pass
