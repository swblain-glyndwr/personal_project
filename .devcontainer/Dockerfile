FROM mcr.microsoft.com/devcontainers/python:3.11

ARG LOCAL_USER

# Remove broken Yarn repo (fix for Debian Trixie base image issue)
RUN rm -f /etc/apt/sources.list.d/yarn.list || true \
 && rm -f /etc/apt/sources.list.d/*yarn* || true

# Update the container
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        build-essential \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspaces

# Global Git identity (matches your host username at build time)
RUN git config --system credential.useHttpPath true \
 && git config --system user.email "${LOCAL_USER}" \
 && git config --system user.name  "${LOCAL_USER}"

# Install Databricks CLI
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
