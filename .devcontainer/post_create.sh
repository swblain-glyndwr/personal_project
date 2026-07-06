#!/bin/bash
set -ex

sudo chown -R vscode:vscode /workspaces/next-ads || true


git config --global --add safe.directory /workspaces/next-ads
git config --global core.autocrlf input

# Copy databricks config
cp .devcontainer/.databrickscfg ~/.databrickscfg

# Install poetry
python3 -m pip install --user --upgrade pip
python3 -m pip install --user poetry==2.2.1
python3 -m poetry config virtualenvs.in-project true

# Install project dependencies
python3 -m poetry install

if [ -z "$USER_SCHEMA" ]; then
  echo "USER_SCHEMA is not set. Create a .env.local file in config/ folder with USER_SCHEMA=<your_user_name_on_databricks>"
  exit 1
fi