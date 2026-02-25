FROM golang:1.21
WORKDIR /app
COPY . .
# RUN go build -o raft-app main.go   # hapus jika sudah binary
CMD ["./adaptive-raft"]
