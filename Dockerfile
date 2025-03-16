# Use the official Go image
FROM golang:1.23.4

WORKDIR /app

COPY . .

RUN go mod tidy

RUN go build -o app

RUN mkdir -p /app/data

EXPOSE 8080

CMD ["/app/app"]
