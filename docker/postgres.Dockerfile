FROM postgres:16

RUN apt-get update && \
    apt-get install -y postgresql-16-pgvector && \
    rm -rf /var/lib/apt/lists/*

ENV POSTGRES_INITDB_ARGS="--encoding=UTF-8 --locale=C.UTF-8"
