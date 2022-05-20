import os
from re import L
import requests

from github import Github
from github import InputGitTreeElement
from collections.abc import Iterable

def push_files(access_token: str, repo_name: str, from_local_fpaths: Iterable,
    to_remote_dpaths: Iterable = "", to_branch: str = "main", commit_msg: str = "",
    file_encodings: Iterable = "utf-8", timeout: int = 15) -> None:
    """ Commit and pushed files from local to remote Github repository branch.

    Parameters:
        access_token (str): Admin access token to Github account for
                authentication purposes.
        repo_name (str): The remote repository name.
        from_local_fpaths (Iterable): The local file paths of the files to be
                committed and pushed.
        to_remote_dpaths (Iterable, str): The relative directory paths within
                the remote repository to push for each file respectively.
                If a single repository directory is given as a string,
                all files will be pushed to this directory. An empty
                directory path maps to the repository parent directory.
        to_branch (str): The branch name to push to.
        commit_msg (str): The commit message to use.
        file_encodings (Iterable, str): The encoding to use for reading the
                file contents. If a single encoding is given as a string,
                all files will be decoded using this encoding format.
        timeout (int): The number of seconds to wait before terminating
                https connection requests.
    """
    def get_local_contents(local_fpath: str, encoding: str) -> any:
        try: 
            with open(local_fpath, encoding=encoding) as inputs:
                contents = inputs.read()
        except: # Fallback on binary read mode
            with open(local_fpath, "rb") as inputs:
                contents = inputs.read()

        return contents

    if isinstance(to_remote_dpaths, str):
        to_remote_dpaths = [ to_remote_dpaths for _ in from_local_fpaths ]

    if isinstance(file_encodings, str):
        file_encodings = [ file_encodings for _ in from_local_fpaths ]

    git_client = Github(access_token, timeout=timeout, retry=5)
    repo = git_client.get_user().get_repo(repo_name)

    branch_ref = repo.get_git_ref(f"heads/{to_branch}")
    branch_tree = repo.get_git_tree(branch_ref.object.sha)
    tree_elements, error_packets = [], []

    for local_fpath, remote_dpath, encoding in zip(from_local_fpaths,
            to_remote_dpaths, file_encodings):
        
        _, fname = os.path.split(local_fpath)
        remote_fpath = f"{remote_dpath}/{fname}" if remote_dpath else fname

        try:
            tree_elements.append(InputGitTreeElement(remote_fpath, "100644",
                    "blob", get_local_contents(local_fpath, encoding)))
        except:
            error_packets.append((local_fpath, remote_fpath, encoding))
    
    if tree_elements:
        tree = repo.create_git_tree(tree_elements, branch_tree)
        parent = repo.get_git_commit(branch_ref.object.sha)
        commit = repo.create_git_commit(commit_msg, tree, [parent])
        branch_ref.edit(commit.sha)

    for local_fpath, remote_fpath, encoding in error_packets:
        # Decoding or compatibility errors
        try: # Remove existing
            repo.delete_file(remote_fpath, commit_msg, repo.get_contents(remote_fpath,
                    ref=to_branch).sha, to_branch)
        except: pass

        repo.create_file(remote_fpath, commit_msg,get_local_contents(local_fpath,
                encoding), to_branch)

def push_directory(access_token: str, repo_name: str, from_local_dpath: str = os.getcwd(),
    to_remote_dpath: str = "", to_branch: str = "main", commit_msg: str = "",
    timeout: int = 15) -> None:
    """ Commit and push directory from local to remote Github repository, preserving
    directory tree structure.

    Parameters:
        access_token (str): Admin access token to Github account for
                authentication purposes.
        repo_name (str): The remote repository name.
        from_local_dpath (str): The local directory path to be committed and pushed.
        to_remote_dpath (str): The relative directory path within the remote
                repository to push the local directory to.
        to_branch (str): The branch name to push to.
        commit_msg (str): The commit message to use.
        timeout (int): The number of seconds to wait before terminating
                https connection requests.
    """
    from_local_fpaths = []
    to_remote_dpaths = []

    for root, _, fnames in os.walk(from_local_dpath):
        for fname in fnames:     
            from_local_fpaths.append(os.path.join(root, fname))
            relative_local_dpath = root.replace(from_local_dpath + os.sep, '') \
                    .replace(from_local_dpath, '') \
                    .replace(os.sep, '/')

            to_remote_dpaths.append(
                '/'.join([
                    path_component for path_component in
                    [to_remote_dpath, relative_local_dpath]
                    if path_component
                ])
            )

    push_files(access_token, repo_name, from_local_fpaths, to_remote_dpaths,
            to_branch, commit_msg, timeout=timeout)

def web_remote_fpath(user_name: str, repo_name: str, remote_fpath: str,
    branch: str = "main") -> str:
    return f"https://raw.githubusercontent.com/{user_name}/{repo_name}/{branch}/{remote_fpath}"

def read_remote_file(user_name: str, repo_name: str, from_remote_fpath: str,
    from_branch: str = "main") -> any:
    """ Reads the content from file in remote Github repository branch.

    Parameters:
        user_name (str): The Github user.
        repo_name (str): The Remote repository name.
        from_remote_fpath (str): The relative filepath within the remote repository.
        from_branch (str): The branch of the repository to pull from.

    Returns:
        fcontent (any): The contents of the remote file.
    """
    page = requests.get(
        web_remote_fpath(user_name, repo_name, from_remote_fpath, from_branch)
    )
    
    return page.content

def pull_directory(user_name: str, repo_name: str, from_remote_dpath: str = "",
    to_local_dpath: str = os.getcwd(), from_branch: str = "main") -> None:
    """ Pulls the contents of a remote Github repository directory.

    Parameters:
        user_name (str): The Github user.
        repo_name (str): The Remote repository name.
        from_remote_dpath (str): The relative directory path within the remote
                repository.
        to_local_dpath (str): The local directory to write the contents into.
        from_branch (str): The branch of the repository to pull from.
    """
    client = Github()
    repo = client.get_repo(f"{user_name}/{repo_name}")

    base_remote_dpath = from_remote_dpath
    base_local_dpath = to_local_dpath

    def pull_directory_walk(from_remote_dpath: str, to_local_dpath: str):
        if not os.path.exists(to_local_dpath):
            os.makedirs(to_local_dpath)

        for element in repo.get_contents(from_remote_dpath, ref=from_branch):
            relative_local_path = element.path.replace(base_remote_dpath, base_local_dpath) \
                    if base_remote_dpath else \
                    os.path.join(base_local_dpath, element.path)
            
            relative_local_path = relative_local_path.replace('/', os.sep)

            if element.type == "dir":
                pull_directory_walk(element.path, relative_local_path)
                continue
            
            fcontent = read_remote_file(user_name, repo_name, element.path,
                    from_branch)

            with open(relative_local_path, 'wb') as fout:
                fout.write(fcontent)

    pull_directory_walk(from_remote_dpath, to_local_dpath)

if __name__ == "__main__":
    pass
