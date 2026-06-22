FROM python:3.12-slim

LABEL org.opencontainers.image.title="M3U2strm2026"
LABEL org.opencontainers.image.description="IPTV VOD to STRM Sync Tool with Web Admin"
LABEL org.opencontainers.image.url="https://github.com/evilgenx/M3U2strm2026"

ENV PYTHONUNBUFFERED=1
ENV M3U2STRM_PLAIN=1
ENV PORT=8080

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN chmod +x /app/docker/entrypoint.sh && mkdir -p /app/data

EXPOSE 8080

ENTRYPOINT ["/app/docker/entrypoint.sh"]