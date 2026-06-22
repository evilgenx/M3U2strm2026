FROM python:3.12-slim

LABEL org.opencontainers.image.title="M3U2strm2026"
LABEL org.opencontainers.image.description="IPTV VOD to STRM Sync Tool with Web Admin"
LABEL org.opencontainers.image.url="https://github.com/evilgenx/M3U2strm2026"

ENV PYTHONUNBUFFERED=1
ENV M3U2STRM_PLAIN=1
ENV PORT=8080

# Build-time args for the non-root user (defaults to 1000:1000)
ARG PUID=1000
ARG PGID=1000

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN chmod +x /app/docker/entrypoint.sh && mkdir -p /app/data

# Create a non-root user with the specified UID & GID so that
# files written to bind-mounted host directories get the correct owner.
RUN if ! getent group "${PGID}" >/dev/null; then \
        groupadd --gid "${PGID}" appgroup; \
    fi && \
    if ! getent passwd "${PUID}" >/dev/null; then \
        useradd --uid "${PUID}" --gid "${PGID}" --no-create-home --shell /bin/false appuser; \
    fi

# Give the app user ownership of /app
RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

ENTRYPOINT ["/app/docker/entrypoint.sh"]
