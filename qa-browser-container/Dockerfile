# syntax=docker/dockerfile:1.7

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY qa-browser-container/github_known_hosts /etc/ssh/ssh_known_hosts

RUN --mount=type=ssh,required=true \
    pip install --no-cache-dir \
    fastapi uvicorn jinja2 python-multipart \
    python-arango pandas pyarrow pyyaml \
    sqlalchemy pymysql psycopg2-binary boto3 openpyxl \
    ijson \
    "ifx-registry[sources] @ git+ssh://git@github.com/ncats/IFX_Registry.git@v0.3.4" \
    obonet networkx rdkit

COPY src ./src

EXPOSE 8050
ENV PYTHONPATH=/app

CMD ["python", "-m", "src.qa_browser.app", "--host", "0.0.0.0"]
