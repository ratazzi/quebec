# syntax=docker/dockerfile:1.7
#
# Image for the cgroup v2 integration suite. Builds the extension inside the
# container so the tests run against a real unified hierarchy; BuildKit cache
# mounts keep the cargo registry and target dir warm between runs.
FROM rust:1-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-dev python3-venv python3-pip pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
# Debian's pip predates `pip install --group`, which `maturin develop` uses to
# resolve [dependency-groups]; upgrading pip is cheaper than pinning maturin.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir "maturin>=1.10,<2.0" \
    && pip install --no-cache-dir "sqlalchemy>=2.0"

# Empty means "use cargo's default". It is exported only when non-empty:
# cargo rejects CARGO_BUILD_JOBS="" rather than ignoring it.
ARG BUILD_JOBS=

WORKDIR /src
COPY Cargo.toml Cargo.lock pyproject.toml README.md ./
COPY src ./src
COPY python ./python

# Build a wheel and install it, rather than `maturin develop`: the wheel path
# does not touch the project's dev dependency groups, which the container has
# no use for.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    sh -c 'if [ -n "$BUILD_JOBS" ]; then export CARGO_BUILD_JOBS="$BUILD_JOBS"; fi; \
           maturin build --out /wheels && pip install --no-cache-dir /wheels/*.whl'

COPY tests/docker/cgroup/scenarios.py /src/scenarios.py

ENV QUEBEC_SKIP_IMPORT_HOOK=1
ENTRYPOINT ["python3", "/src/scenarios.py"]
