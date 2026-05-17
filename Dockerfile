# -------构建阶段--------
FROM golang:1.25-alpine AS builder
 
ENV GOPROXY=https://goproxy.cn,direct
ENV GO111MODULE=on

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o redis-go .

# -------运行阶段--------
FROM alpine:latest

WORKDIR /app

RUN mkdir -p /app/data

COPY --from=builder /app/redis-go ./

EXPOSE 6380 6060

CMD ["./redis-go", "-addr", ":6380", "-pprof", ":6060"]
