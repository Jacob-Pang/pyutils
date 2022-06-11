import os

from github import Repository
from github import InputGitTreeElement
from collections.abc import Iterable
from .common import github_relative_path, repository_walk

def write_files(authenticated_repo: Repository, file_contents: Iterable,
    to_remote_file_paths: Iterable, branch: str = "main", commit_message: str = '',
    push_batch_size: int = 20) -> None:
    """ Commit and write files into remote GitHub repository

    Parameters:
        authenticated_repo (Repository): Authenticated repository object.
        file_contents (Iterable): The contents of the files to write.
        to_remote_file_paths (Iterable): The relative file paths within
                the remote repository to push for each file respectively.
        branch (str): The branch name to push to.
        commit_message (str): The commit message to use.
        push_batch_size (int): The maximum number of files to push within
                one commit, and partition the files by.
    """
    if len(file_contents) > push_batch_size:
        write_files(authenticated_repo, file_contents[push_batch_size:],
                to_remote_file_paths[push_batch_size:], branch, commit_message,
                push_batch_size)
        
        file_contents = file_contents[:push_batch_size]
        to_remote_file_paths = to_remote_file_paths[:push_batch_size]

    branch_reference = authenticated_repo.get_git_ref(f"heads/{branch}")
    branch_tree = authenticated_repo.get_git_tree(branch_reference.object.sha)
    tree_elements, file_exceptions = [], []

    for file_content, remote_file_path in zip(file_contents, to_remote_file_paths):
        try:
            tree_elements.append(InputGitTreeElement(remote_file_path,
                    "100644", "blob", file_content))
        except:
            file_exceptions.append((file_content, remote_file_path))
    
    if tree_elements:
        git_tree = authenticated_repo.create_git_tree(tree_elements, branch_tree)
        parent = authenticated_repo.get_git_commit(branch_reference.object.sha)
        commit = authenticated_repo.create_git_commit(commit_message, git_tree, [parent])
        branch_reference.edit(commit.sha)

    for file_content, remote_file_path in file_exceptions:
        try: # Remove existing file
            authenticated_repo.delete_file(remote_file_path, commit_message, authenticated_repo
                    .get_contents(remote_file_path, ref=branch).sha, branch)
        except: pass

        authenticated_repo.create_file(remote_file_path, commit_message, file_content, branch)

def push_files(authenticated_repo: Repository, from_local_file_paths: Iterable,
    to_remote_directory_paths: Iterable = '', branch: str = "main", commit_message: str = "",
    file_encodings: Iterable = "utf-8", push_batch_size: int = 20) -> None:
    """ Commit and push files from local directory to remote Github repository.

    Parameters:
        authenticated_repo (Repository): Authenticated repository object.
        from_local_file_paths (Iterable): The local file paths of the files to be
                committed and pushed.
        to_remote_directory_paths (Iterable, str): The relative directory paths within
                the remote repository to push for each file respectively.
                If a single repository directory is given as a string,
                all files will be pushed to this directory. An empty
                directory path maps to the repository parent directory.
        branch (str): The branch name to push to.
        commit_message (str): The commit message to use.
        file_encodings (Iterable, str): The encoding to use for reading the
                file contents. If a single encoding is given as a string,
                all files will be decoded using this encoding format.
        push_batch_size (int): The maximum number of files to push within
                one commit, and partition the files by.
    """
    def get_local_file_contents(local_file_path: str, file_encoding: str) -> any:
        try:
            with open(local_file_path, encoding=file_encoding) as inputs:
                contents = inputs.read()
        except: # Fallback on binary read mode
            with open(local_file_path, "rb") as inputs:
                contents = inputs.read()

        return contents

    if isinstance(to_remote_directory_paths, str):
        to_remote_directory_paths = [ to_remote_directory_paths for _ in from_local_file_paths ]

    if isinstance(file_encodings, str):
        file_encodings = [ file_encodings for _ in from_local_file_paths ]

    file_contents, to_remote_file_paths = [], []

    for local_file_path, remote_directory_path, file_encoding in zip(from_local_file_paths,
            to_remote_directory_paths, file_encodings):
        
        _, file_name = os.path.split(local_file_path)
        remote_file_path = github_relative_path(f"{remote_directory_path}/{file_name}")

        file_contents.append(get_local_file_contents(local_file_path, file_encoding))
        to_remote_file_paths.append(remote_file_path)

    write_files(authenticated_repo, file_contents, to_remote_file_paths,
            branch, commit_message, push_batch_size)

def push_directory(authenticated_repo: Repository, from_local_directory_path: str = os.getcwd(),
    to_remote_directory_path: str = '', branch: str = "main", commit_message: str = "",
    push_batch_size: int = 20) -> None:
    """ Commit and push directory from local to remote Github repository, preserving
    directory tree structure.

    Parameters:
        authenticated_repo (Repository): Authenticated repository object.
        from_local_directory_path (str): The local directory path to be committed and pushed.
        to_remote_directory_path (str): The relative directory path within the remote
                repository to push the local directory to.
        branch (str): The branch name to push to.
        commit_message (str): The commit message to use.
        timeout (int): The number of seconds to wait before terminating
                https connection requests.
        push_batch_size (int): The maximum number of files to push within
                one commit, and partition the files by.
    """
    from_local_file_paths, to_remote_directory_paths = [], []

    for root, _, file_names in os.walk(from_local_directory_path):
        for file_name in file_names:     
            from_local_file_paths.append(os.path.join(root, file_name))
            relative_local_dpath = github_relative_path(root, from_local_directory_path)

            to_remote_directory_paths.append(
                '/'.join([
                    path_component for path_component in
                    [to_remote_directory_path, relative_local_dpath]
                    if path_component
                ])
            )

    push_files(authenticated_repo, from_local_file_paths, to_remote_directory_paths,
            branch, commit_message, push_batch_size=push_batch_size)

def delete_file(authenticated_repo: Repository, remote_file_path: str, branch: str = "main",
    commit_message: str = '') -> None:
    """
    """
    for remote_file_content in repository_walk(authenticated_repo, remote_file_path, branch):
        authenticated_repo.delete_file(remote_file_content.path, commit_message,
                remote_file_content.sha, branch)

if __name__ == "__main__":
    pass
