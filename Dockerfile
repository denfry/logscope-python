FROM python:3.12.8-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY app ./app
RUN pip install --no-cache-dir .
COPY migrations ./migrations

RUN useradd --create-home --uid 10001 logscope \
    && chown -R logscope:logscope /app
USER logscope

EXPOSE 8080
CMD ["python", "-m", "app.main"]
