FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc curl && rm -rf /var/lib/apt/lists/*

# Python deps
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY backend/ ./backend/
COPY apps/web/ ./apps/web/

# keep project root on PYTHONPATH so "backend" package is importable
WORKDIR /app

EXPOSE 8000

# run as package so relative imports resolve correctly
CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
