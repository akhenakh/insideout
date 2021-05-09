FROM gcr.io/distroless/static

WORKDIR /root/
COPY insided .
COPY grpc_health_probe .
ADD static /root/static
ADD inside.db /root/inside.db
EXPOSE 8080 6666 8088 9200
ENTRYPOINT ["./insided"]
