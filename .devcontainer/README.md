# Development Container for `next-ads`

This devcontainer is configured for the `next-ads` project (requires Python >= 3.11). It is intended to provide the smoothest dev experience possible and support best practices for ML Ops

How it works:
- Uses the official VS Code Python devcontainer base image for Python 3.11.
- Databricks Extension sets up the virtual environment

What do you get
- Linux containerised environment (doesn't affect your windows setup and can run linux tooling/ you can install stuff)
- Databricks extension set up so you can interactively code with a databricks cluster in vs code
- Script to install the helper library
- Linting that will pick up the local environment
- pytest tests will run against a cluster
- Precommit hooks for linting and formatting your code (and more in future)

Quick start:
1. In VS Code: `Dev Containers: Reopen in Container`.
2. Interactively log in to databricks by running `databricks auth login` in the terminal, this will create a profile, that you can optionally name. This stores the details of the interaction, so that you don't have to keep entering the url etc. After this, you can refer to the profile by this name, e.g. when performing databricks CLI commands

Databricks Extension
1. Hit ctrl+shift+p, and run `Databricks: Sign in to Databricks workspace`, then choose the profile you created in step 2 of the quick start (if you didn't name it, it's called "[DEFAULT]"). Wait a while... as it will now be creating a python environment for you
2. Run `cd .devcontainer` to go to the folder and run `bash install_dsutils.sh` to install the helper library required for the project
3. In the extension, choose a cluster compatible with this project, it should be on runtime 15.4, as this is in line with the project, start it if it's stopped
4. To check everything is set up, run the tests by going to the tests section of vs code, this will be using connections to databricks and this environment

Bonus
- There is an option to `Setup Databricks builtins for autocompletion` in the configuration of the extension, click this to get autocomplete working, watch your notifications to allow this
- If you need additional system packages (Debian apt packages), add them to the `Dockerfile`'s `apt-get install` line.
