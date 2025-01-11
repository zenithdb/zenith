ARG REPOSITORY=neondatabase
ARG COMPUTE_IMAGE=compute-node-v14
ARG TAG=latest

FROM $REPOSITORY/${COMPUTE_IMAGE}:$TAG

ARG COMPUTE_IMAGE

USER root
RUN apt-get update &&       \
    apt-get install -y curl \
                       jq   \
                       python3-pip \
                       netcat-openbsd
#Faker is required for the pg_anon test
RUN case $COMPUTE_IMAGE in compute-node-v17) OPT="--break-system-packages";; *) OPT= ;; esac && pip3 install $OPT Faker
#This is required for the pg_hintplan test
RUN mkdir -p /ext-src/pg_hint_plan-src && chown postgres /ext-src/pg_hint_plan-src 
#This is required for the PostGIS test
RUN mkdir -p /ext-src/postgis-src && chown postgres /ext-src/postgis-src

USER postgres