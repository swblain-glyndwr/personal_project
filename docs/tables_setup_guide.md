## Project Tables Setup Guide

This guide walks new developers through setting up the required tables for the project.

### Prerequisites
- Access to Databricks workspace
- Python environment configured
- Setup script available in `scripts/table_operations/` directory

### Setup Instructions

1. **Set your Databricks user schema**
    Create a `.env.local` file in `configs/runtime/`:
    ```
    USER_SCHEMA=your_user_name_on_databricks
    ```
    Replace `your_user_name_on_databricks` with your actual Databricks username (e.g., `john_doe`)

2. **Run the table creation script**
    ```bash
    # sample mode
    bash setup_dev_tables.sh --sample # this creates starting tables with limited data

    # OR use full init
    bash setup_dev_tables.sh

    # Tip - the dev tables setup requires a correctly configured environment. It runs commands against python, if your python in shell is not your virtual environment interpreter, it won't work. But you can use poetry to run it and that will ensure the script is running correctly in your virtual env:

    poetry run setup_dev_tables.sh

    # ORRRR.... use the python version, then you will likely not have any env issues as your venv should be active and you can optionally run it in the python debugger

    poetry run setup_dev_tables.py
    ```

### Next Steps

Once the script completes successfully, your project tables will be created in your personal Databricks schema. You can now begin development work.

For troubleshooting or questions, refer to the main project README.
export USER_SCHEMA="your_user_name_on_databricks" # e.g. john_doe
python scripts/create_tables.py --job_env dev --log_level INFO

### Environment Modes

#### Development Mode (`dev`)
In development mode, your personal user schema is used for all table writes. This allows each developer to work in isolation without affecting shared infrastructure.

- **User schema requirement**: You must set `USER_SCHEMA` environment variable to your Databricks username
- **Write location**: Tables are written to `marketingdata_dev` catalog in your personal schema (e.g., `@format {env[USER_SCHEMA]}`)
- **Use case**: Local development, testing, and experimentation

#### Production/Preprod Modes (`prod`, `preprod`)
In non-development environments, tables are written to shared, centralized locations using default schemas.

- **User schema**: Not required; ignored if set
- **Write location**: Tables are written to `marketingdata_prod` catalog in shared warehouse schemas (`ds_sandbox` or `warehouse`)
- **Use case**: Production data processing, shared analysis, and deployments

**Key difference**: Always ensure you're using `dev` mode during development to avoid writing to production tables.
