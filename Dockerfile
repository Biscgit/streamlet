ARG VERSION_TAG=python3.13

FROM registry.cern.ch/inveniosoftware/almalinux:latest AS cert

WORKDIR /cert
RUN cp /etc/pki/tls/certs/ca-bundle.crt bundle.crt
RUN echo "# CERN Certification Authority 2" >> bundle.crt &&  \
    curl -s "https://ca.cern.ch/cafiles/certificates/CERN%20Certification%20Authority(2).crt" >> bundle.crt && \
    echo "CERN Grid Certification Authority" >>  bundle.crt && \
    curl -s "https://ca.cern.ch/cafiles/certificates/CERN%20Grid%20Certification%20Authority(1).crt" >> bundle.crt

FROM ghcr.io/astral-sh/uv:${VERSION_TAG}-alpine AS builder

RUN apk add build-base openssl-dev git curl # gcc librdkafka-dev
WORKDIR /app

COPY pyproject.toml .
RUN uv sync

COPY src/ src/
RUN uv sync

WORKDIR /etc/ssl/certs/
COPY --from=cert /cert/bundle.crt bundle.crt

FROM ghcr.io/astral-sh/uv:${VERSION_TAG}-alpine
LABEL authors="biscgit"

RUN apk update && apk upgrade && apk add openssl openssl-dev --no-cache && rm -rf /var/cache/apk/*
RUN addgroup -S nonroot-group && adduser -S streamlet -G nonroot-group

COPY --from=builder /app /app
COPY --from=builder /etc/ssl/certs/bundle.crt /etc/ssl/certs/bundle.crt

RUN mkdir -p /tmp/uv/ && \
    chmod -R 777 /tmp/uv

WORKDIR /app

RUN uv pip install --no-cache-dir --no-deps .
USER streamlet

ENV TMPDIR=/tmp
ENV UV_CACHE_DIR=/tmp/uv

ENTRYPOINT ["uv", "run", "--directory", "/app", "src/main.py"]
