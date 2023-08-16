FROM messense/rust-musl-cross:x86_64-musl AS ktwb-builder
WORKDIR /src
COPY . .
RUN cargo build --release

FROM scratch
WORKDIR /app
COPY --from=ktwb-builder /src/target/x86_64-unknown-linux-musl/release/kafka-to-worterbuch .
ENV RUST_LOG=info
ENV WORTERBUCH_PROTO=ws
ENV WORTERBUCH_HOST_ADDRESS=worterbuch.local
ENV WORTERBUCH_PORT=80
ENTRYPOINT ["./kafka-to-worterbuch"]
