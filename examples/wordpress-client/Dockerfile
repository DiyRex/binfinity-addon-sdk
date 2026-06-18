# Presserve — Binfinity WordPress edge addon image. Build context is the REPO ROOT
# (bundles the binfinity CLI built from the monorepo + this addon):
#   docker build -f addons/wordpress-client/Dockerfile -t diyrex5224/binfinity-wp-addon:latest .
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
RUN cd clients/cli && CGO_ENABLED=0 go build -o /out/binfinity .
RUN cd addons/wordpress-client && CGO_ENABLED=0 GOWORK=off go build -o /out/wordpress-edge .

# Runtime: official MySQL image provides a matching mysqldump/mysql client.
FROM mysql:8.4
COPY --from=build /out/binfinity /usr/local/bin/binfinity
COPY --from=build /out/wordpress-edge /usr/local/bin/wordpress-edge
# Credentials live under HOME (default /state), never inside WP_CONTENT (the backup
# root). Do NOT hardcode CRED_PATH — the SDK derives it from HOME ($HOME/credentials.json),
# so a deploy that persists creds on a volume just sets HOME to the mount (e.g.
# HOME=/data). An explicit CRED_PATH would override HOME and defeat that (creds land
# on the ephemeral container fs → the addon re-enrolls as a NEW system every restart).
ENV HOME=/state WP_PATH=/var/www/html
RUN mkdir -p /state && chown mysql:mysql /state
USER mysql
WORKDIR /state
ENTRYPOINT ["/usr/local/bin/wordpress-edge"]
