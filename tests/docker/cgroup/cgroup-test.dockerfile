# syntax=docker/dockerfile:1.7
#
# Image for the cgroup v2 integration suite. Builds the extension inside the
# container so the tests run against a real unified hierarchy; BuildKit cache
# mounts keep the cargo registry and target dir warm between runs.
FROM rust:1-slim-bookworm

COPY --from=ghcr.io/jdx/mise:2026.9.1 /usr/local/bin/mise /usr/local/bin/mise
COPY --from=ghcr.io/astral-sh/uv:0.12.13 /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl pkg-config \
    && rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION=3.11
ENV MISE_DATA_DIR=/opt/mise
ENV UV_PYTHON_DOWNLOADS=never
ENV VIRTUAL_ENV=/opt/venv
RUN mise install "python@${PYTHON_VERSION}" \
    && mise exec "python@${PYTHON_VERSION}" -- uv venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN uv pip install --no-cache "maturin>=1.10,<2.0" "sqlalchemy>=2.0"

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
           maturin build --out /wheels && uv pip install --no-cache /wheels/*.whl'

COPY tests/docker/cgroup/scenarios.py /src/scenarios.py

ENV QUEBEC_SKIP_IMPORT_HOOK=1
ENTRYPOINT ["python3", "/src/scenarios.py"]
