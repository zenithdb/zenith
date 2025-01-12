### Creates a storage Docker image with postgres, pageserver, safekeeper and proxy binaries.
### The image itself is mainly used as a container for the binaries and for starting e2e tests with custom parameters.
### By default, the binaries inside the image have some mock parameters and can start, but are not intended to be used
### inside this image in the real deployments.
ARG REPOSITORY=neondatabase
ARG IMAGE=build-tools
ARG TAG=pinned
ARG DEFAULT_PG_VERSION=17
ARG STABLE_PG_VERSION=16
ARG DEBIAN_VERSION=bookworm
ARG DEBIAN_FLAVOR=${DEBIAN_VERSION}-slim

# Build Postgres
FROM $REPOSITORY/$IMAGE:$TAG AS pg-build
WORKDIR /home/nonroot

COPY --chown=nonroot vendor/postgres-v14 vendor/postgres-v14
COPY --chown=nonroot vendor/postgres-v15 vendor/postgres-v15
COPY --chown=nonroot vendor/postgres-v16 vendor/postgres-v16
COPY --chown=nonroot vendor/postgres-v17 vendor/postgres-v17
COPY --chown=nonroot pgxn pgxn
COPY --chown=nonroot Makefile Makefile
COPY --chown=nonroot scripts/ninstall.sh scripts/ninstall.sh

ENV BUILD_TYPE=release
RUN set -e \
    && mold -run make -j $(nproc) -s neon-pg-ext \
    && rm -rf pg_install/build \
    && tar -C pg_install -czf /home/nonroot/postgres_install.tar.gz .

# Build neon binaries
FROM $REPOSITORY/$IMAGE:$TAG AS build
WORKDIR /home/nonroot
ARG GIT_VERSION=local
ARG BUILD_TAG
ARG STABLE_PG_VERSION

COPY --from=pg-build /home/nonroot/pg_install/v14/include/postgresql/server pg_install/v14/include/postgresql/server
COPY --from=pg-build /home/nonroot/pg_install/v15/include/postgresql/server pg_install/v15/include/postgresql/server
COPY --from=pg-build /home/nonroot/pg_install/v16/include/postgresql/server pg_install/v16/include/postgresql/server
COPY --from=pg-build /home/nonroot/pg_install/v17/include/postgresql/server pg_install/v17/include/postgresql/server
COPY --from=pg-build /home/nonroot/pg_install/v16/lib                       pg_install/v16/lib
COPY --from=pg-build /home/nonroot/pg_install/v17/lib                       pg_install/v17/lib
COPY --chown=nonroot . .

ARG ADDITIONAL_RUSTFLAGS
RUN set -e \
    && PQ_LIB_DIR=$(pwd)/pg_install/v${STABLE_PG_VERSION}/lib RUSTFLAGS="-Clinker=clang -Clink-arg=-fuse-ld=mold -Clink-arg=-Wl,--no-rosegment -Cforce-frame-pointers=yes ${ADDITIONAL_RUSTFLAGS}" cargo build \
      --bin pg_sni_router  \
      --bin pageserver  \
      --bin pagectl  \
      --bin safekeeper  \
      --bin storage_broker  \
      --bin storage_controller  \
      --bin proxy  \
      --bin neon_local \
      --bin storage_scrubber \
      --locked --release

# Build final image
#
FROM debian:${DEBIAN_FLAVOR}
ARG DEFAULT_PG_VERSION
WORKDIR /data

RUN set -e \
    && apt update \
    && apt install -y \
        libreadline-dev \
        libseccomp-dev \
        ca-certificates \
	# System postgres for use with client libraries (e.g. in storage controller)
        postgresql-15 \
        openssl \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && useradd -d /data neon \
    && chown -R neon:neon /data

COPY --from=build --chown=neon:neon /home/nonroot/target/release/pg_sni_router       /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/pageserver          /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/pagectl             /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/safekeeper          /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/storage_broker      /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/storage_controller  /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/proxy               /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/neon_local          /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/storage_scrubber    /usr/local/bin

COPY --from=pg-build /home/nonroot/pg_install/v14 /usr/local/v14/
COPY --from=pg-build /home/nonroot/pg_install/v15 /usr/local/v15/
COPY --from=pg-build /home/nonroot/pg_install/v16 /usr/local/v16/
COPY --from=pg-build /home/nonroot/pg_install/v17 /usr/local/v17/
COPY --from=pg-build /home/nonroot/postgres_install.tar.gz /data/

# By default, pageserver uses `.neon/` working directory in WORKDIR, so create one and fill it with the dummy config.
# Now, when `docker run ... pageserver` is run, it can start without errors, yet will have some default dummy values.
RUN mkdir -p /data/.neon/ && \
  echo "id=1234" > "/data/.neon/identity.toml" && \
  echo "broker_endpoint='http://storage_broker:50051'\n" \
       "pg_distrib_dir='/usr/local/'\n" \
       "listen_pg_addr='0.0.0.0:6400'\n" \
       "listen_http_addr='0.0.0.0:9898'\n" \
       "availability_zone='local'\n" \
  > /data/.neon/pageserver.toml && \
  chown -R neon:neon /data/.neon

VOLUME ["/data"]
USER neon
EXPOSE 6400
EXPOSE 9898

CMD ["/usr/local/bin/pageserver", "-D", "/data/.neon"]

