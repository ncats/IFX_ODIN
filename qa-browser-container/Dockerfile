FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi uvicorn jinja2 python-multipart \
    python-arango pandas pyarrow pyyaml

COPY src ./src

EXPOSE 8050
ENV PYTHONPATH="/app:${PYTHONPATH}"

CMD ["python", "-m", "src.qa_browser.app", "--host", "0.0.0.0"]