#!/bin/bash
# create tables
SAMPLE_FLAG=""
[[ "$1" == "--sample" ]] && SAMPLE_FLAG=1

echo "Creating tables..."
python scripts/table_operations/create_tables.py --job_env dev --log_level INFO  

if [[ $SAMPLE_FLAG -eq 1 ]]; then
    echo "Running in sample mode..."
    python scripts/table_operations/init_starting_tables.py 

else
    echo "Running in standard mode..."

    # populate key tables
    TODAY=$(date +%Y-%m-%d)
    echo "Using date $TODAY for populating tables"
    echo "Running parse_attributes..."
    python scripts/parse_attributes.py --job_env dev --log_level INFO  --refresh_attributes_date $TODAY

    echo "Running parse_theme_mapping..."
    python scripts/parse_theme_mapping.py --job_env dev --log_level INFO  --refresh_themes_date $TODAY

    echo "Running build_markov_chain..."
    python scripts/build_markov_chain.py --job_env dev --log_level INFO  --refresh_model_date $TODAY
fi

