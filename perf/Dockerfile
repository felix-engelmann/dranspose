FROM rust:1.77 as build

# create a new empty shell project
RUN USER=root cargo new --bin fast_ingester
WORKDIR /fast_ingester

# copy over your manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# this build step will cache your dependencies
RUN cargo build --release
RUN rm src/*.rs

# copy your source tree
COPY ./src ./src

# build for release
RUN rm ./target/release/deps/fast_ingester*
RUN cargo build --release

# our final base
FROM debian:12-slim

# copy the build artifact from the build stage
COPY --from=build /fast_ingester/target/release/fast_ingester .

# set the startup command to run your binary
CMD ["./fast_ingester"]
