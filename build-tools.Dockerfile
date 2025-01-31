ARG DEBIAN_VERSION=bookworm

FROM debian:bookworm-slim AS pgcopydb_builder
ARG DEBIAN_VERSION

RUN echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries && \
    echo -e "retry_connrefused = on\ntimeout=15\ntries=5\n" > /root/.wgetrc \
    echo -e "--retry-connrefused\n--connect-timeout 15\n--retry 5\n--max-time 300\n" > /root/.curlrc

RUN if [ "${DEBIAN_VERSION}" = "bookworm" ]; then \
        set -e && \
        apt update && \
        apt install -y --no-install-recommends \
        ca-certificates wget gpg && \
        wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg && \
        echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
        apt-get update && \
        apt install -y --no-install-recommends \
        build-essential \
        autotools-dev \
        libedit-dev \
        libgc-dev \
        libpam0g-dev \
        libreadline-dev \
        libselinux1-dev \
        libxslt1-dev \
        libssl-dev \
        libkrb5-dev \
        zlib1g-dev \
        liblz4-dev \
        libpq5 \
        libpq-dev \
        libzstd-dev \
        postgresql-16 \
        postgresql-server-dev-16 \
        postgresql-common  \
        python3-sphinx && \
        wget -O /tmp/pgcopydb.tar.gz https://github.com/dimitri/pgcopydb/archive/refs/tags/v0.17.tar.gz && \
        mkdir /tmp/pgcopydb && \
        tar -xzf /tmp/pgcopydb.tar.gz -C /tmp/pgcopydb --strip-components=1 && \
        cd /tmp/pgcopydb && \
        make -s clean && \
        make -s -j12 install && \
        libpq_path=$(find /lib /usr/lib -name "libpq.so.5" | head -n 1) && \
        mkdir -p /pgcopydb/lib && \
        cp "$libpq_path" /pgcopydb/lib/; \
    else \
        # copy command below will fail if we don't have dummy files, so we create them for other debian versions
        mkdir -p /usr/lib/postgresql/16/bin && touch /usr/lib/postgresql/16/bin/pgcopydb && \
        mkdir -p mkdir -p /pgcopydb/lib && touch /pgcopydb/lib/libpq.so.5; \
    fi

FROM debian:${DEBIAN_VERSION}-slim AS build_tools
ARG DEBIAN_VERSION

# Add nonroot user
RUN useradd -ms /bin/bash nonroot -b /home
SHELL ["/bin/bash", "-c"]

RUN mkdir -p /pgcopydb/bin && \
    mkdir -p /pgcopydb/lib && \
    chmod -R 755 /pgcopydb && \
    chown -R nonroot:nonroot /pgcopydb

COPY --from=pgcopydb_builder /usr/lib/postgresql/16/bin/pgcopydb /pgcopydb/bin/pgcopydb
COPY --from=pgcopydb_builder /pgcopydb/lib/libpq.so.5 /pgcopydb/lib/libpq.so.5

RUN echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries && \
    echo -e "retry_connrefused = on\ntimeout=15\ntries=5\n" > /root/.wgetrc \
    echo -e "--retry-connrefused\n--connect-timeout 15\n--retry 5\n--max-time 300\n" > /root/.curlrc

# System deps
#
# 'gdb' is included so that we get backtraces of core dumps produced in
# regression tests
RUN set -e \
    && apt update \
    && apt install -y \
        autoconf \
        automake \
        bison \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        flex \
        gdb \
        git \
        gnupg \
        gzip \
        jq \
        jsonnet \
        libcurl4-openssl-dev \
        libbz2-dev \
        libffi-dev \
        liblzma-dev \
        libncurses5-dev \
        libncursesw5-dev \
        libreadline-dev \
        libseccomp-dev \
        libsqlite3-dev \
        libssl-dev \
        $([[ "${DEBIAN_VERSION}" = "bullseye" ]] && echo libstdc++-10-dev || echo libstdc++-11-dev) \
        libtool \
        libxml2-dev \
        libxmlsec1-dev \
        libxxhash-dev \
        lsof \
        make \
        netcat-openbsd \
        net-tools \
        openssh-client \
        parallel \
        pkg-config \
        unzip \
        wget \
        xz-utils \
        zlib1g-dev \
        zstd \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# sql_exporter

# Keep the version the same as in compute/compute-node.Dockerfile and
# test_runner/regress/test_compute_metrics.py.
ENV SQL_EXPORTER_VERSION=0.17.0
RUN curl -fsSL \
    "https://github.com/burningalchemist/sql_exporter/releases/download/${SQL_EXPORTER_VERSION}/sql_exporter-${SQL_EXPORTER_VERSION}.linux-$(case "$(uname -m)" in x86_64) echo amd64;; aarch64) echo arm64;; esac).tar.gz" \
    --output sql_exporter.tar.gz \
    && mkdir /tmp/sql_exporter \
    && tar xzvf sql_exporter.tar.gz -C /tmp/sql_exporter --strip-components=1 \
    && mv /tmp/sql_exporter/sql_exporter /usr/local/bin/sql_exporter

# protobuf-compiler (protoc)
ENV PROTOC_VERSION=25.1
RUN curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-$(uname -m | sed 's/aarch64/aarch_64/g').zip" -o "protoc.zip" \
    && unzip -q protoc.zip -d protoc \
    && mv protoc/bin/protoc /usr/local/bin/protoc \
    && mv protoc/include/google /usr/local/include/google \
    && rm -rf protoc.zip protoc

# s5cmd
ENV S5CMD_VERSION=2.2.2
RUN curl -sL "https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}/s5cmd_${S5CMD_VERSION}_Linux-$(uname -m | sed 's/x86_64/64bit/g' | sed 's/aarch64/arm64/g').tar.gz" | tar zxvf - s5cmd \
    && chmod +x s5cmd \
    && mv s5cmd /usr/local/bin/s5cmd

# LLVM
ENV LLVM_VERSION=19
RUN curl -fsSL 'https://apt.llvm.org/llvm-snapshot.gpg.key' | apt-key add - \
    && echo "deb http://apt.llvm.org/${DEBIAN_VERSION}/ llvm-toolchain-${DEBIAN_VERSION}-${LLVM_VERSION} main" > /etc/apt/sources.list.d/llvm.stable.list \
    && apt update \
    && apt install -y clang-${LLVM_VERSION} llvm-${LLVM_VERSION} \
    && bash -c 'for f in /usr/bin/clang*-${LLVM_VERSION} /usr/bin/llvm*-${LLVM_VERSION}; do ln -s "${f}" "${f%-${LLVM_VERSION}}"; done' \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install docker
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian ${DEBIAN_VERSION} stable" > /etc/apt/sources.list.d/docker.list \
    && apt update \
    && apt install -y docker-ce docker-ce-cli \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Configure sudo & docker
RUN usermod -aG sudo nonroot && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    usermod -aG docker nonroot

# AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip" \
    && unzip -q awscliv2.zip \
    && ./aws/install \
    && rm awscliv2.zip

# Mold: A Modern Linker
ENV MOLD_VERSION=v2.34.1
RUN set -e \
    && git clone https://github.com/rui314/mold.git \
    && mkdir mold/build \
    && cd mold/build \
    && git checkout ${MOLD_VERSION} \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++ .. \
    && cmake --build . -j $(nproc) \
    && cmake --install . \
    && cd .. \
    && rm -rf mold

# LCOV
# Build lcov from a fork:
# It includes several bug fixes on top on v2.0 release (https://github.com/linux-test-project/lcov/compare/v2.0...master)
# And patches from us:
# - Generates json file with code coverage summary (https://github.com/neondatabase/lcov/commit/426e7e7a22f669da54278e9b55e6d8caabd00af0.tar.gz)
RUN for package in Capture::Tiny DateTime Devel::Cover Digest::MD5 File::Spec JSON::XS Memory::Process Time::HiRes JSON; do yes | perl -MCPAN -e "CPAN::Shell->notest('install', '$package')"; done \
    && wget https://github.com/neondatabase/lcov/archive/426e7e7a22f669da54278e9b55e6d8caabd00af0.tar.gz -O lcov.tar.gz \
    && echo "61a22a62e20908b8b9e27d890bd0ea31f567a7b9668065589266371dcbca0992  lcov.tar.gz" | sha256sum --check \
    && mkdir -p lcov && tar -xzf lcov.tar.gz -C lcov --strip-components=1 \
    && cd lcov \
    && make install \
    && rm -rf ../lcov.tar.gz

# Use the same version of libicu as the compute nodes so that
# clusters created using inidb on pageserver can be used by computes.
#
# TODO: at this time, compute-node.Dockerfile uses the debian bullseye libicu
# package, which is 67.1. We're duplicating that knowledge here, and also, technically,
# Debian has a few patches on top of 67.1 that we're not adding here.
ENV ICU_VERSION=67.1
ENV ICU_PREFIX=/usr/local/icu

# Download and build static ICU
RUN wget -O /tmp/libicu-${ICU_VERSION}.tgz https://github.com/unicode-org/icu/releases/download/release-${ICU_VERSION//./-}/icu4c-${ICU_VERSION//./_}-src.tgz && \
    echo "94a80cd6f251a53bd2a997f6f1b5ac6653fe791dfab66e1eb0227740fb86d5dc /tmp/libicu-${ICU_VERSION}.tgz" | sha256sum --check && \
    mkdir /tmp/icu && \
    pushd /tmp/icu && \
    tar -xzf /tmp/libicu-${ICU_VERSION}.tgz && \
    pushd icu/source && \
    ./configure --prefix=${ICU_PREFIX}  --enable-static --enable-shared=no CXXFLAGS="-fPIC" CFLAGS="-fPIC" && \
    make -j "$(nproc)" && \
    make install && \
    popd && \
    rm -rf icu && \
    rm -f /tmp/libicu-${ICU_VERSION}.tgz && \
    popd

# Switch to nonroot user
USER nonroot:nonroot
WORKDIR /home/nonroot

RUN echo -e "--retry-connrefused\n--connect-timeout 15\n--retry 5\n--max-time 300\n" > /home/nonroot/.curlrc

# Python
ENV PYTHON_VERSION=3.11.10 \
    PYENV_ROOT=/home/nonroot/.pyenv \
    PATH=/home/nonroot/.pyenv/shims:/home/nonroot/.pyenv/bin:/home/nonroot/.poetry/bin:$PATH
RUN set -e \
    && cd $HOME \
    && curl -sSO https://raw.githubusercontent.com/pyenv/pyenv-installer/master/bin/pyenv-installer \
    && chmod +x pyenv-installer \
    && ./pyenv-installer \
    && export PYENV_ROOT=/home/nonroot/.pyenv \
    && export PATH="$PYENV_ROOT/bin:$PATH" \
    && export PATH="$PYENV_ROOT/shims:$PATH" \
    && pyenv install ${PYTHON_VERSION} \
    && pyenv global ${PYTHON_VERSION} \
    && python --version \
    && pip install --upgrade pip \
    && pip --version \
    && pip install pipenv wheel poetry

# Switch to nonroot user (again)
USER nonroot:nonroot
WORKDIR /home/nonroot

# Rust
# Please keep the version of llvm (installed above) in sync with rust llvm (`rustc --version --verbose | grep LLVM`)
ENV RUSTC_VERSION=1.84.0
ENV RUSTUP_HOME="/home/nonroot/.rustup"
ENV PATH="/home/nonroot/.cargo/bin:${PATH}"
ARG RUSTFILT_VERSION=0.2.1
ARG CARGO_HAKARI_VERSION=0.9.33
ARG CARGO_DENY_VERSION=0.16.2
ARG CARGO_HACK_VERSION=0.6.33
ARG CARGO_NEXTEST_VERSION=0.9.85
RUN curl -sSO https://static.rust-lang.org/rustup/dist/$(uname -m)-unknown-linux-gnu/rustup-init && whoami && \
	chmod +x rustup-init && \
	./rustup-init -y --default-toolchain ${RUSTC_VERSION} && \
	rm rustup-init && \
    export PATH="$HOME/.cargo/bin:$PATH" && \
    . "$HOME/.cargo/env" && \
    cargo --version && rustup --version && \
    rustup component add llvm-tools rustfmt clippy && \
    cargo install rustfilt            --version ${RUSTFILT_VERSION} && \
    cargo install cargo-hakari        --version ${CARGO_HAKARI_VERSION} && \
    cargo install cargo-deny --locked --version ${CARGO_DENY_VERSION} && \
    cargo install cargo-hack          --version ${CARGO_HACK_VERSION} && \
    cargo install cargo-nextest       --version ${CARGO_NEXTEST_VERSION} && \
    rm -rf /home/nonroot/.cargo/registry && \
    rm -rf /home/nonroot/.cargo/git

# Show versions
RUN whoami \
    && python --version \
    && pip --version \
    && cargo --version --verbose \
    && rustup --version --verbose \
    && rustc --version --verbose \
    && clang --version

RUN if [ "${DEBIAN_VERSION}" = "bookworm" ]; then \
    LD_LIBRARY_PATH=/pgcopydb/lib /pgcopydb/bin/pgcopydb --version; \
else \
    echo "pgcopydb is not available for ${DEBIAN_VERSION}"; \
fi

# Set following flag to check in Makefile if its running in Docker
RUN touch /home/nonroot/.docker_build
