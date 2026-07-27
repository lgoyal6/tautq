# Build context is the PARENT directory holding both checkouts side by side (taut/ and
# tautq/) — tautq vendors taut from its sibling path. docker-compose.yml sets this up.
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang cmake ninja-build git ca-certificates && rm -rf /var/lib/apt/lists/*
COPY taut /src/taut
COPY tautq /src/tautq
WORKDIR /src/tautq
RUN cmake --preset release -DTAUTQ_BUILD_TESTS=OFF && cmake --build --preset release

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    iptables iproute2 curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/tautq/build/release/tautq-node /usr/local/bin/tautq-node
COPY --from=build /src/tautq/build/release/tautq-worker /usr/local/bin/tautq-worker
COPY --from=build /src/tautq/build/release/tautq-sink /usr/local/bin/tautq-sink
COPY --from=build /src/tautq/build/release/tautq-verify /usr/local/bin/tautq-verify
