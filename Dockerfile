FROM python:3.11-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /data-pipeline/

COPY .python-version /data-pipeline/.python-version
COPY pyproject.toml /data-pipeline/pyproject.toml
COPY uv.lock /data-pipeline/uv.lock

RUN uv sync --only-group base
RUN uv run crawl4ai-setup

COPY src /data-pipeline/src
COPY assets /data-pipeline/assets

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
