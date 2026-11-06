FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONPATH=/opt/platform/src DATA_ROOT=/data
WORKDIR /opt/platform
COPY requirements/app.lock /tmp/app.lock
RUN pip install --no-cache-dir --require-hashes -r /tmp/app.lock && pip check && useradd -u 50000 -g 0 -m platform
COPY src ./src
COPY dbt ./dbt
COPY demo ./demo
COPY dashboard ./dashboard
COPY scripts ./scripts
USER 50000:0
ENTRYPOINT ["python", "-m", "turnaround.cli"]
