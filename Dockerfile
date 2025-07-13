FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy application
COPY . /app

# Install dependencies
WORKDIR /app
RUN uv sync --frozen --no-cache

# Command to run
CMD ["uv", "run", "app/main.py"]
