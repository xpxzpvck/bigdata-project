FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --no-dev

COPY . /app/

CMD ["uv", "run", "python", "ingestion.py"]