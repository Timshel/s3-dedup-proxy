FROM docker.io/library/debian:bookworm-slim AS build

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        gzip \
        openjdk-17-jre \
    && curl -o /usr/share/keyrings/sbt.asc "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
    && echo "deb [signed-by=/usr/share/keyrings/sbt.asc] https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends sbt

COPY . /s3-dedup-proxy
WORKDIR /s3-dedup-proxy

RUN git submodule init && git submodule update
RUN sbt stage

FROM docker.io/library/debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        openjdk-17-jre

COPY --from=build /s3-dedup-proxy/target/universal/stage /s3-dedup-proxy

WORKDIR /s3-dedup-proxy

ENTRYPOINT ["/s3-dedup-proxy/bin/s3-dedup-proxy"]
