FROM golang:1.26.4 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/openflux . \
    && mkdir -p /out/state \
    && chmod 0700 /out/state

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/openflux /usr/local/bin/openflux
COPY --from=build --chown=65532:65532 /out/state /state
COPY LICENSE NOTICE /usr/share/licenses/openflux/

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/openflux"]
CMD ["--help"]
