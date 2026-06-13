# Binfinity MySQL edge addon image (example). Build context = the SDK repo root:
#   docker build -f examples/mysql-client/Dockerfile -t binfinity-mysql-edge .
#
# This builds the agent (which uses the Addon SDK). The agent shells out to the
# `binfinity` CLI for the BSP data plane — that binary ships separately with the
# Binfinity platform release. Provide it one of two ways:
#   (a) drop the binary at examples/mysql-client/binfinity before building, OR
#   (b) mount it at runtime: docker run -v /path/to/binfinity:/usr/local/bin/binfinity ...
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
RUN cd examples/mysql-client && CGO_ENABLED=0 GOWORK=off go build -o /out/mysql-edge .

# Runtime: official MySQL image provides matching mysqldump/mysql clients.
FROM mysql:8.4
COPY --from=build /out/mysql-edge /usr/local/bin/mysql-edge
# Uncomment if using option (a) above:
# COPY examples/mysql-client/binfinity /usr/local/bin/binfinity
ENV HOME=/data
RUN mkdir -p /data && chown mysql:mysql /data
USER mysql
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/mysql-edge"]
