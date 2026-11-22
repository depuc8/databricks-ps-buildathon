# Shopping Sessions: a native Rust pipeline (see src/main.rs).
# x86-64-v3 (AVX2, BMI2) is safe on the scorers. The build is guided by a
# profile (pgo/merged.profdata, made with this rustc from runs on 20M- and
# 1M-event datasets): the profile must come from the same rustc version.
FROM rust:1.100.0-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq -C profile-use=/src/pgo/merged.profdata"
RUN cargo build --release --locked

FROM debian:bookworm-slim
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
WORKDIR /app
CMD ["/app/shopping-sessions"]
