# Start from a minimal Python image
FROM python:3.11-slim

# Set environment vars
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    python3-dev \
    libssl-dev \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*


# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy app code
COPY src ./src

# Expose the app port
EXPOSE 8000
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Use gunicorn with uvicorn workers
CMD ["gunicorn", "src.use_cases.api.host_graphql_api:app", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]

