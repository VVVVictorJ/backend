# syntax=docker/dockerfile:1
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System deps (if pandas / scientific stack is used)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app

# Install Python deps (from pyproject declared deps)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir fastapi>=0.127.0 httpx>=0.28.1 pandas>=2.3.3 pydantic>=2.12.5 uvicorn>=0.40.0

# Expose internal port
EXPOSE 8000

# Healthcheck (optional but useful)
HEALTHCHECK --interval=30s --timeout=5s --retries=5 CMD python -c "import socket; s=socket.socket(); s.settimeout(3); s.connect(('127.0.0.1',8000)); s.close()"

# Run uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]


