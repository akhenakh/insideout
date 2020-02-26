FROM gcr.io/distroless/static

WORKDIR /root/
COPY insided .
COPY grpc_health_probe .
COPY static/ .
COPY out.db/ .
ENTRYPOINT ["./insided"]
