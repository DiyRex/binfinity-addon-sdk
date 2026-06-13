# Binfinity files/folder edge addon image (example). Build context = repo root:
#   docker build -f examples/files-client/Dockerfile -t binfinity-files-edge .
# Provide the `binfinity` CLI binary (ships with the Binfinity platform release):
# drop it at examples/files-client/binfinity and uncomment the COPY, or mount it
# at runtime (-v /path/to/binfinity:/usr/local/bin/binfinity).
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
RUN cd examples/files-client && CGO_ENABLED=0 GOWORK=off go build -o /out/files-edge .

# Runtime: slim base with tar.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends tar ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/files-edge /usr/local/bin/files-edge
# COPY examples/files-client/binfinity /usr/local/bin/binfinity
ENV HOME=/data
RUN mkdir -p /data
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/files-edge"]
