#!/bin/bash
set -ex
git config --global --add safe.directory /workspaces/next-ads
git config --global core.autocrlf input

# Copy databricks config
cp .devcontainer/.databrickscfg ~/.databrickscfg

# Install poetry
python3 -m pip install --user --upgrade pip
python3 -m pip install --user poetry==2.2.1

# Install project dependencies
python3 -m poetry install
