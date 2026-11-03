FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /producer .

FROM gcr.io/distroless/static-debian12
COPY --from=build /producer /producer
ENTRYPOINT ["/producer"]
