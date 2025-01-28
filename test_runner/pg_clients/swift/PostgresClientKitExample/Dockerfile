FROM swift:5.10@sha256:2351780014bad244d1867e03ef282508aedbc438b0a722ea126eac5c6d104131 AS build
RUN apt-get -q update && apt-get -q install -y libssl-dev
WORKDIR /source

COPY . .
RUN swift build --configuration release

FROM swift:5.10@sha256:2351780014bad244d1867e03ef282508aedbc438b0a722ea126eac5c6d104131
WORKDIR /app
COPY --from=build /source/.build/release .
CMD ["/app/PostgresClientKitExample"]
