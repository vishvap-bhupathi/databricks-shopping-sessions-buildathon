FROM python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df

COPY requirements-docker.txt /app/requirements.txt
RUN python3 -m pip install --no-cache-dir --disable-pip-version-check \
        --require-hashes --only-binary=:all: --no-deps -r /app/requirements.txt \
    && groupadd --gid 1000 app \
    && useradd --uid 1000 --gid 1000 --no-create-home --home-dir /scratch app \
    && install -d /data \
    && install -d -o 1000 -g 1000 /output /scratch

COPY starter-duckdb/starter-duckdb/pipeline/*.py /app/pipeline/
COPY starter-duckdb/starter-duckdb/run.sh /app/run.sh

ENV DATA_DIR=/data \
    OUTPUT_DIR=/output \
    SCRATCH_DIR=/scratch \
    HOME=/scratch \
    NCPU=8 \
    MEM_GB=24 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
USER 1000:1000
CMD ["/app/run.sh"]
