#!/bin/bash
set -ex
git config --global --add safe.directory /workspaces/next-ads
git config --global core.autocrlf input

# Copy databricks config
cp .devcontainer/.databrickscfg ~/.databrickscfg

# Install poetry
python3 -m pip install --user --upgrade pip
python3 -m pip install --user poetry==2.2.1

# Configure Poetry for private MarketingDataFeed repository
echo "Configuring Poetry for private package repository..."
read -p "Enter Azure DevOps username: " AZURE_DEVOPS_USERNAME
read -sp "Enter Azure DevOps PAT (Personal Access Token): " AZURE_DEVOPS_PAT
echo

poetry config http-basic.MarketingDataFeed "$AZURE_DEVOPS_USERNAME" "$AZURE_DEVOPS_PAT"

# Install project dependencies
python3 -m poetry install
