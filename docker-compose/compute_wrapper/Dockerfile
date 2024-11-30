ARG REPOSITORY=neondatabase
ARG COMPUTE_IMAGE=compute-node-v14
ARG TAG=latest

FROM $REPOSITORY/${COMPUTE_IMAGE}:$TAG

USER root
RUN apt-get update &&       \
    apt-get install -y curl \
                       jq   \
                       python3-pip \
                       netcat-openbsd
#Faker is required for the pg_anon test
RUN pip3 install --break-system-packages Faker
#This is required for the pg_hintplan test
RUN mkdir -p /ext-src/pg_hint_plan-src && chown postgres /ext-src/pg_hint_plan-src

USER postgres
