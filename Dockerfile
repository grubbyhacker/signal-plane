FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/signal-gateway ./cmd/signal-gateway \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/signal-observer ./cmd/signal-observer

FROM gcr.io/distroless/static-debian12

COPY --from=build /out/signal-gateway /usr/local/bin/signal-gateway
COPY --from=build /out/signal-observer /usr/local/bin/signal-observer

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/signal-gateway"]
