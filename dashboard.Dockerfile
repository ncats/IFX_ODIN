FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    python3-dev \
    libssl-dev \
    libpq-dev

WORKDIR /app

# Copy your app and dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501
ENV PYTHONPATH="/app:${PYTHONPATH}"

CMD ["streamlit", "run", "src/use_cases/dashboard/host_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
