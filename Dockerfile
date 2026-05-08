# -------构建阶段--------
FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o redis-go .

# -------运行阶段--------
FROM scratch
COPY --from=builder /build/redis-go /redis-go
EXPOSE 6380
ENTRYPOINT ["/redis-go"]