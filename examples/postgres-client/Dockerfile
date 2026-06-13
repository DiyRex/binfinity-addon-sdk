# Binfinity PostgreSQL edge addon image (example). Build context = repo root:
#   docker build -f examples/postgres-client/Dockerfile -t binfinity-postgres-edge .
# Provide the `binfinity` CLI binary (ships with the Binfinity platform release):
# drop it at examples/postgres-client/binfinity and uncomment the COPY, or mount
# it at runtime (-v /path/to/binfinity:/usr/local/bin/binfinity).
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
RUN cd examples/postgres-client && CGO_ENABLED=0 GOWORK=off go build -o /out/postgres-edge .

# Runtime: postgres image provides matching pg_dump / psql clients.
FROM postgres:16
COPY --from=build /out/postgres-edge /usr/local/bin/postgres-edge
# COPY examples/postgres-client/binfinity /usr/local/bin/binfinity
ENV HOME=/data
RUN mkdir -p /data
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/postgres-edge"]
