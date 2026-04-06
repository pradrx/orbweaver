FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/parser ./cmd/parser

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
    && adduser -D -H appuser
USER appuser
COPY --from=build /bin/parser /bin/parser
ENTRYPOINT ["/bin/parser"]
