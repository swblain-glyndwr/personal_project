#!/bin/bash
echo "Setting Databricks Asset Bundle deployment metadata..."
set -e

# Get git commit SHA
export BUNDLE_VAR_git_commit_sha=$(git rev-parse HEAD 2>/dev/null || echo "unknown")

# Get git branch - Azure DevOps already provides clean name
if [ -n "${BUILD_SOURCEBRANCHNAME:-}" ]; then
  export BUNDLE_VAR_git_branch="${BUILD_SOURCEBRANCHNAME}"
else
  # Local development fallback
  export BUNDLE_VAR_git_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
fi

# Get deployed by (user or CI/CD)
if [ -n "${BUILD_REQUESTEDFOR:-}" ]; then
  # CI/CD deployment
  export BUNDLE_VAR_deployed_by="${BUILD_REQUESTEDFOR}"
else
  # Local user deployment
  export BUNDLE_VAR_deployed_by=$(git config user.email 2>/dev/null || whoami)
fi

# Get deployment timestamp
export BUNDLE_VAR_deploy_timestamp=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

echo "📦 Databricks Asset Bundle Deployment Metadata (auto-detected):"
echo "  Timestamp: ${BUNDLE_VAR_deploy_timestamp}"
echo "  Commit: ${BUNDLE_VAR_git_commit_sha}"
echo "  Branch: ${BUNDLE_VAR_git_branch}"
echo "  Deployed by: ${BUNDLE_VAR_deployed_by}"