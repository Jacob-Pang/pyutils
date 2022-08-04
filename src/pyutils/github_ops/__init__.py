import os
import requests

from github import Github, Repository

def get_repository(repository_name: str, user_name: str) -> Repository:
    return Github().get_repo(f"{user_name}/{repository_name}")

def get_authenticated_repository(repository_name: str, github_user: Github = None,
    access_token: str = None) -> Repository:
    if not github_user:
        github_user = Github(access_token)
    
    return github_user.get_repo(repository_name)

def github_address(user_name: str, repository_name: str, remote_file_path: str, branch: str = "main") -> str:
    return f"https://github.com/{user_name}/{repository_name}/blob/{branch}/{remote_file_path}"

def raw_github_address(user_name: str, repository_name: str, remote_file_path: str, branch: str = "main") -> str:
    return f"https://raw.githubusercontent.com/{user_name}/{repository_name}/{branch}/{remote_file_path}"

def address_exists(address: str) -> bool:
    return requests.get(address).status_code != 404

def github_relative_path(path: str, root_directory_path: str = None) -> str:
    if root_directory_path:
        path = path.replace(root_directory_path, '')

    return path.replace(os.path.sep, '/').strip('/')

def repository_walk(repository: Repository, from_remote_directory_path: str = '', branch: str = "main") -> list:
    remote_directory_paths = [from_remote_directory_path] # Queue
    remote_file_contents = []

    while remote_directory_paths:
        remote_directory_path = remote_directory_paths.pop()
        remote_directory_contents = repository.get_contents(remote_directory_path, ref=branch)

        if not isinstance(remote_directory_contents, list): # file_path
            remote_file_contents.append(remote_directory_contents)
            continue

        for remote_file_content in remote_directory_contents:
            if remote_file_content.type == "dir":
                remote_directory_paths.append(remote_file_content.path)
                continue

            remote_file_contents.append(remote_file_content)

    return remote_file_contents

if __name__ == "__main__":
    pass