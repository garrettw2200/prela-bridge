FROM golang:1.24 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o prela-bridge -ldflags="-w -s" .

FROM gcr.io/distroless/static
COPY --from=builder /app/prela-bridge /usr/local/bin/prela-bridge
ENTRYPOINT ["/usr/local/bin/prela-bridge"]
