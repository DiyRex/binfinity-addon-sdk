# Binfinity WordPress edge addon image (example). Build context = repo root:
#   docker build -f examples/wordpress-client/Dockerfile -t binfinity-wordpress-edge .
# Provide the `binfinity` CLI binary (ships with the Binfinity platform release):
# drop it at examples/wordpress-client/binfinity and uncomment the COPY, or mount
# it at runtime (-v /path/to/binfinity:/usr/local/bin/binfinity).
#
# Mount the WordPress site at WP_PATH (e.g. -v wp_html:/var/www/html) so the addon
# can read wp-content and write the .maintenance flag.
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
RUN cd examples/wordpress-client && CGO_ENABLED=0 GOWORK=off go build -o /out/wordpress-edge .

# Runtime: mysql image provides mysqldump/mysql clients; tar + cp come with the base.
FROM mysql:8.4
COPY --from=build /out/wordpress-edge /usr/local/bin/wordpress-edge
# COPY examples/wordpress-client/binfinity /usr/local/bin/binfinity
ENV HOME=/data
RUN mkdir -p /data
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/wordpress-edge"]
