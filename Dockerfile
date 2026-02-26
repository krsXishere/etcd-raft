##############################################
# Stage 1 – Build the Go binaries
##############################################
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Cache module download
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build both binaries
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o adaptive-raft main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o benchmark   cmd/benchmark/main.go

##############################################
# Stage 2 – Runtime image
##############################################
FROM alpine:3.19

# iproute2 provides the `tc` command for traffic control (netem)
RUN apk add --no-cache iproute2 bash curl

WORKDIR /app

COPY --from=builder /app/adaptive-raft .
COPY --from=builder /app/benchmark    .
COPY scripts/entrypoint.sh            /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh /app/adaptive-raft /app/benchmark

# Default: run the raft node via entrypoint (applies TC, then starts node)
ENTRYPOINT ["/app/entrypoint.sh"]
