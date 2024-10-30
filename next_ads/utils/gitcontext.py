from pathlib import Path
import os


def find_git_head() -> Path:

    git_dir = Path(os.getcwd()).parent

    while not (git_dir/".git").exists():
        git_dir_prev = git_dir
        git_dir = git_dir.parent
        if git_dir == git_dir_prev:
            raise Exception("No .git directory not found")

    return (git_dir/".git"/"HEAD")


def get_active_git_branch() -> str:

    head_dir = find_git_head()
    with head_dir.open("r") as f:
        content = f.read().splitlines()

    for line in content:
        if line[0:4] == "ref:":
            return line.partition("refs/heads/")[2]
