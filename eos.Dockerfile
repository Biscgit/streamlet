FROM registry.cern.ch/inveniosoftware/almalinux:latest AS cert

WORKDIR /cert
RUN cp /etc/pki/tls/certs/ca-bundle.crt bundle.crt
RUN echo "# CERN Certification Authority 2" >> bundle.crt &&  \
    curl -s "https://ca.cern.ch/cafiles/certificates/CERN%20Certification%20Authority(2).crt" >> bundle.crt && \
    echo "CERN Grid Certification Authority" >>  bundle.crt && \
    curl -s "https://ca.cern.ch/cafiles/certificates/CERN%20Grid%20Certification%20Authority(1).crt" >> bundle.crt

FROM almalinux:9-minimal
LABEL authors="biscgit"

COPY eos.repo /etc/yum.repos.d/eos.repo

RUN microdnf update -y && \
    microdnf install -y epel-release git && \
    microdnf install -y eos-client && \
    microdnf clean all -y && \
    rm -rf /var/lib/rpm/*

RUN python3 -m ensurepip && \
    python3 -m pip install --upgrade uv pip setuptools

COPY --from=registry.cern.ch/inveniosoftware/almalinux:latest /etc/pki/tls/certs/ca-bundle.crt /etc/ssl/certs/bundle.crt

WORKDIR /etc/ssl/certs/
COPY --from=cert /cert/bundle.crt bundle.crt

ENV TMPDIR=/tmp
ENV UV_CACHE_DIR=/tmp/uv/

RUN mkdir -p /tmp/uv/

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/

RUN uv sync
RUN uv pip install --no-cache-dir --no-deps .

RUN chgrp -R 0 /tmp/uv && \
    chmod -R g=u /tmp/uv && \
    useradd streamlet --uid 1000 --gid 0 && \
    chown -R streamlet:root /tmp/uv

ENTRYPOINT ["uv", "run", "--directory", "/app", "src/main.py"]
