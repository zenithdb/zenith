ARG REPOSITORY=neondatabase
ARG COMPUTE_IMAGE=compute-node-v14
ARG TAG=latest

FROM $REPOSITORY/${COMPUTE_IMAGE}:$TAG

ARG COMPUTE_IMAGE

USER root
RUN echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries && \
    apt-get update &&       \
    apt-get install -y curl \
                       jq   \
                       netcat-openbsd
#This is required for the pg_hintplan test
RUN mkdir -p /ext-src/pg_hint_plan-src /postgres/contrib/file_fdw && chown postgres /ext-src/pg_hint_plan-src /postgres/contrib/file_fdw

USER postgres
