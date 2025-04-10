FROM python:3.12-slim

RUN pip install --upgrade pip && pip install uv

WORKDIR /app

COPY pyproject.toml README.md ./

RUN uv pip install --system --no-cache .[infra]

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME

WORKDIR $DAGSTER_HOME
