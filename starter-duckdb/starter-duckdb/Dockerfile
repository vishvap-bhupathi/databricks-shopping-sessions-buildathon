# Shopping Sessions starter: DuckDB skeleton.
# The scorers are linux/amd64 (Intel Xeon). They build this file natively, so
# no FROM line pins a platform: `make build` builds for linux/amd64 anywhere.
FROM rust:1.85-bookworm@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS parser
COPY native/strict_csv.rs native/events.rs native/engine.rs /
RUN rustc --crate-type cdylib -O -C panic=abort /strict_csv.rs -o /libstrict_csv.so
RUN rustc --cfg native_engine -O /strict_csv.rs -o /native-engine

FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=parser /libstrict_csv.so /app/pipeline/libstrict_csv.so

COPY --from=parser /native-engine /app/native-engine

WORKDIR /app
CMD ["/app/run.sh"]
