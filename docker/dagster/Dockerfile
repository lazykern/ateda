FROM python:3.12-slim

# Install uv first
RUN pip install --upgrade pip && pip install uv

WORKDIR /app

COPY pyproject.toml README.md ./

# Install only base dependencies + the 'infra' optional dependencies using uv
RUN uv pip install --system --no-cache .[infra]

# Set $DAGSTER_HOME
ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME

# The workspace.yaml and dagster.yaml will be mounted as volumes
WORKDIR $DAGSTER_HOME
