FROM python:3.11-slim AS builder

WORKDIR /app
RUN pip install --no-cache-dir poetry==2.3.4
COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.in-project true \
    && poetry install --only main --no-root --no-interaction

COPY src ./src
COPY README.md .
RUN poetry install --no-interaction

FROM python:3.11-slim

WORKDIR /app
RUN useradd --create-home --uid 1000 mini_cache
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
ENV PATH="/app/.venv/bin:$PATH"

RUN mkdir -p /data && chown mini_cache:mini_cache /data
USER mini_cache
VOLUME ["/data"]

EXPOSE 6380
ENTRYPOINT ["mini_cache"]
CMD ["--host", "0.0.0.0", "--port", "6380", "--aof-path", "/data/mini_cache.aof"]