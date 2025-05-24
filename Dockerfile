FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory to parent of your app
WORKDIR /

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Copy your entire backend as a package
COPY . /app/

# Add the parent directory to Python path
ENV PYTHONPATH=/

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser
RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Run uvicorn with the full module path
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]