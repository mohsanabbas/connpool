# Stage 1
FROM golang:1.26 AS build

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/example ./example

# Stage 2
FROM gcr.io/distroless/static-debian12

COPY --from=build /bin/example /example
ENTRYPOINT ["/example"]
