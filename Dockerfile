FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg postgresql-common \
 && /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y \
 && apt-get update \
 && apt-get install -y --no-install-recommends postgresql-17 postgresql-client-17 \
 && rm -rf /var/lib/apt/lists/* /var/lib/postgresql/17/main

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && useradd --uid 10001 --create-home deex

COPY --chown=root:root . .
RUN chmod 0555 /app/scripts/start_deex.sh \
 && mkdir -p /app/snapshot \
 && chown -R 10001:10001 /app/snapshot

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DEEX_PG_BIN=/usr/lib/postgresql/17/bin
USER 10001:10001
CMD ["/app/scripts/start_deex.sh"]
