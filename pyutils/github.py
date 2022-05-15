import os

from github import Github
from github import InputGitTreeElement
from collections.abc import Iterable

def push_files(client: Github, repo_name: str, file_paths: Iterable,
    repo_dpaths: Iterable = "", master_branch = "main",
    commit_message: str = "") -> None:
    """ Commit and pushes files to remote Github repository

    Parameters:
        client (Github): Github client.
        repo_name (str): The remote repository name.
        file_paths (Iterable): The local file paths of the files to be
                committed and pushed.
        repo_dpaths (Iterable, str): The relative directory paths within
                the remote repository to push for each file respectively.
                If a single repository directory is given as a string,
                all files will be pushed to this directory. An empty
                directory path maps to the repository parent directory.
        master_branch (str): The name of the master branch.
        commit_message (str): The commit message to use.
    """
    if isinstance(repo_dpaths, str):
        repo_dpaths = [ repo_dpaths for _ in file_paths ]

    repo = client.get_user().get_repo(repo_name)
    master_ref = repo.get_git_ref(f"heads/{master_branch}")
    base_tree = repo.get_git_tree(master_ref.object.sha)

    input_tree_elements = []

    for file_path, repo_dpath in zip(file_paths, repo_dpaths):
        with open(file_path) as inputs:
            data = inputs.read()

        _, file_name = os.path.split(file_path)
        repo_fpath = f"{repo_dpath}/{file_name}" if repo_dpath else file_name

        input_tree_elements.append(
            InputGitTreeElement(repo_fpath, "100644", "blob", data)
        )

    tree = repo.create_git_tree(input_tree_elements, base_tree)
    parent = repo.get_git_commit(master_ref.object.sha)
    commit = repo.create_git_commit(commit_message, tree, [parent])
    master_ref.edit(commit.sha)

if __name__ == "__main__":
    pass
