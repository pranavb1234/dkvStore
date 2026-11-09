# Build a small static node binary, then run it in Alpine for simple health checks.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/dkvnode ./cmd/dkvnode

FROM alpine:3.20
RUN adduser -D -H -u 10001 dkv
USER dkv
COPY --from=build /out/dkvnode /usr/local/bin/dkvnode
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dkvnode"]
