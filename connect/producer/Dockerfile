# syntax=docker/dockerfile:1

FROM python:3.12-slim AS base

# Create non-root user
RUN groupadd -r app && useradd -r -g app -u 1000 app
WORKDIR /app

# Copy source
COPY --chown=app:app . /app

# Install runtime deps if requirements present
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

USER app

CMD ["python", "weather_producer.py"]
