FROM rust:1.75 as builder

ADD ./src ./builder/src
ADD ./Cargo.toml ./builder/Cargo.toml
ADD ./tests/sqllogictest ./builder/tests/sqllogictest

WORKDIR /builder

RUN rustup default nightly
RUN cargo build --release

FROM debian:12.5-slim

RUN echo "deb https://mirrors.tuna.tsinghua.edu.cn/debian/ buster main contrib non-free" > /etc/apt/sources.list && \
    echo "deb-src https://mirrors.tuna.tsinghua.edu.cn/debian/ buster main contrib non-free" >> /etc/apt/sources.list
RUN apt-get update && apt-get install -y postgresql-client

ARG APP_SERVER=kite_sql

WORKDIR /kite_sql

EXPOSE 5432

COPY --from=builder /builder/target/release/${APP_SERVER} ${APP_SERVER}

ENTRYPOINT ["./kite_sql", "--ip", "0.0.0.0"]