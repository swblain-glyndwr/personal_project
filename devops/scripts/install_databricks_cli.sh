#!/bin/bash

echo "Uninstall databricks cli or config, if present"

if [ -f '/usr/local/bin/databricks' ]; then
    sudo rm /usr/local/bin/databricks
fi
            
if [ -f '~/.databrickscfg' ]; then
    rm ~/.databrickscfg
fi

echo "Install databricks cli"
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

echo "Databricks CLI version:"
databricks --version

echo "=== Creating Databricks CLI Configuration ==="

# Create .databrickscfg directory if it doesn't exist
mkdir -p ~/.databricks

# Create .databrickscfg file with DEV profile
cat > ~/.databrickscfg << EOF
[DEFAULT]
host = ${DATABRICKS_HOST}
client_id = ${DATABRICKS_CLIENT_ID}
client_secret = ${DATABRICKS_CLIENT_SECRET}
auth_type = oauth-m2m
EOF

# Set proper permissions
chmod 600 ~/.databrickscfg

echo "✓ Databricks configuration file created"
echo ""

unset DATABRICKS_CLUSTER_ID # this is because we loaded this from DevOps variables

# Verify authentication works
echo "Testing authentication with DEV profile:"
databricks auth env
echo ""