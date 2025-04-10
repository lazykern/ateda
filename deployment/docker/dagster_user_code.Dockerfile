FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    aria2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip && pip install uv

WORKDIR /opt/ateda_platform

COPY pyproject.toml README.md* ./

RUN uv pip install . --system --no-cache

COPY src/ateda_platform ./ateda_platform

EXPOSE 4000

CMD ["dagster", "api", "grpc", "--host", "0.0.0.0", "--port", "4000", "--module-name", "ateda_platform.definitions", "--attribute", "defs"]