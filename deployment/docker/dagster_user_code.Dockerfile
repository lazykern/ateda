FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    aria2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip && pip install uv

WORKDIR /app

COPY pyproject.toml README.md* ./

RUN uv pip install . --system --no-cache

# Copy the entire src directory generically
COPY src /app/src 

# Add the source directory to PYTHONPATH so modules can be found
ENV PYTHONPATH="${PYTHONPATH}:/app/src"

EXPOSE 4000

CMD ["dagster", "api", "grpc", "--host", "0.0.0.0", "--port", "4000", "--module-name", "ateda_platform.definitions", "--attribute", "defs"]