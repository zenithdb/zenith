FROM rust:1.79@sha256:9b2689d6f99ff381f178fa4361db745c8c355faecde73aa5b18b0efa84f03e62
WORKDIR /source

COPY . .
RUN cargo build
CMD ["/source/target/debug/rust-neon-example"]
