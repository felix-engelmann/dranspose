FROM rust:1.77 as build

# create a new empty shell project
RUN USER=root cargo new --bin fast_ingester
WORKDIR /fast_ingester

# copy over your manifests
COPY ./perf/Cargo.lock ./Cargo.lock
COPY ./perf/Cargo.toml ./Cargo.toml

# this build step will cache your dependencies
RUN cargo build --release
RUN rm src/*.rs

# copy your source tree
COPY ./perf/src ./src

# build for release
RUN rm ./target/release/deps/fast_ingester*
RUN cargo build --release


FROM python:3

WORKDIR /tmp

COPY . /tmp

COPY --from=build /fast_ingester/target/release/fast_ingester /bin/

RUN python -m pip --no-cache-dir install ".[bench]"

CMD ["dranspose"]
