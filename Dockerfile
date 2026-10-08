FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/rackforest-snapshot-api ./src/main.go

FROM golang:1.26-bookworm AS test
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
CMD ["go", "test", "-race", "./src/..."]

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rackforest-snapshot-api /rackforest-snapshot-api
EXPOSE 3000
USER nonroot
ENTRYPOINT ["/rackforest-snapshot-api"]
