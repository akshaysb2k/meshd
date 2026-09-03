FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/meshd   ./cmd/meshd \
 && CGO_ENABLED=0 go build -o /out/meshcp  ./cmd/meshcp \
 && CGO_ENABLED=0 go build -o /out/backend ./cmd/backend \
 && CGO_ENABLED=0 go build -o /out/loadgen ./cmd/loadgen

FROM alpine:3.20
RUN apk add --no-cache curl ca-certificates
COPY --from=build /out/ /usr/local/bin/
COPY examples/ /etc/meshd/
EXPOSE 8080 9901
ENTRYPOINT ["/usr/local/bin/meshd"]
