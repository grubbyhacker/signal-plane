FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/signal-gateway ./cmd/signal-gateway \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/signal-observer ./cmd/signal-observer \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/github-task-dispatcher ./cmd/github-task-dispatcher \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/resume-release-router ./cmd/resume-release-router \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/push-security-scanner ./cmd/push-security-scanner

FROM gcr.io/distroless/static-debian12

COPY --from=build /out/signal-gateway /usr/local/bin/signal-gateway
COPY --from=build /out/signal-observer /usr/local/bin/signal-observer
COPY --from=build /out/github-task-dispatcher /usr/local/bin/github-task-dispatcher
COPY --from=build /out/resume-release-router /usr/local/bin/resume-release-router
COPY --from=build /out/push-security-scanner /usr/local/bin/push-security-scanner

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/signal-gateway"]
