FROM golang:1.27.4-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/control-plane ./cmd/control-plane && \
    CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -o /out/bootstrap ./cmd/bootstrap && \
    CGO_ENABLED=0 go build -trimpath -o /out/cpctl ./cmd/cpctl

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
# The secret store volume inherits this directory's ownership when Docker
# creates it, so the unprivileged runtime user can write tenant secrets.
RUN mkdir -p /data/secrets && chown -R 65532:65532 /data
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
CMD ["control-plane"]
