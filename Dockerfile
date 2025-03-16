# Use the official Go image
FROM golang:1.24.1

WORKDIR /app

COPY . .

RUN go mod tidy

RUN go build -o app

RUN mkdir -p /app/data

EXPOSE 8080
EXPOSE 6060
CMD ["/app/app"]
