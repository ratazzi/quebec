#!/usr/bin/env bash
#
# cgroup v2 integration suite. Builds the image, runs the scenarios against a
# real unified hierarchy, and checks the non-privileged degradation paths.
# Exits non-zero if anything fails.
#
#   tests/docker/cgroup/run.sh
#
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
IMAGE="quebec-cgroup-test"
FAILURES=0

note() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$*"; FAILURES=$((FAILURES + 1)); }

note "docker host"
docker info --format 'cgroup {{.CgroupVersion}} / driver {{.CgroupDriver}}' || {
    echo "docker unavailable"; exit 1;
}

note "build image"
# --progress=plain so a long cold compile shows progress instead of looking
# wedged. CARGO_BUILD_JOBS caps parallelism; the default saturated this host
# hard enough to hang the daemon once.
DOCKER_BUILDKIT=1 docker build --progress=plain \
    --build-arg BUILD_JOBS="${CARGO_BUILD_JOBS:-}" \
    -f "$REPO_ROOT/tests/docker/cgroup/cgroup-test.dockerfile" \
    -t "$IMAGE" "$REPO_ROOT" || { echo "build failed"; exit 1; }

# The real hierarchy needs a writable /sys/fs/cgroup, which on Docker means
# --privileged; ro is the default and cannot be lifted with SYS_ADMIN alone.
#
# One container per scenario, deliberately: the supervisor must be the only
# process in the container cgroup, otherwise cgroup v2 refuses to enable
# controllers there. A shared driver process would sit in that same cgroup.
for scenario in oom no_limits cleanup derive observe rolling_restart placement_failure; do
    note "scenario: $scenario (--cgroupns=private --privileged)"
    if docker run --rm --cgroupns=private --privileged "$IMAGE" run "$scenario"; then
        pass "scenario $scenario"
    else
        fail "scenario $scenario"
    fi
done

# Phase 2 metrics are read-only, so they must survive the default ro mount
# with no privileges at all. This is the case Kubernetes actually gives us.
note "scenario: observe_readonly (no privileges, ro /sys/fs/cgroup)"
if docker run --rm --cgroupns=private "$IMAGE" run observe_readonly; then
    pass "scenario observe_readonly"
else
    fail "scenario observe_readonly"
fi

# Degradation: same image, no privileges, so /sys/fs/cgroup is read-only.
note "degradation: limits configured, cgroup unavailable -> startup error"
CONFIG='test:
  workers:
    - queues: "*"
      processes: 1
      memory_max: 128MiB
'
OUTPUT=$(docker run --rm --cgroupns=private \
    -e QUEBEC_ENV=test \
    -e QUEBEC_INLINE_CONFIG="$CONFIG" \
    --entrypoint /bin/sh "$IMAGE" -c \
    'mkdir -p /cfg && printf "%s" "$QUEBEC_INLINE_CONFIG" > /cfg/queue.yml
     QUEBEC_CONFIG=/cfg/queue.yml exec python3 /src/scenarios.py supervisor-boot' 2>&1)
STATUS=$?
echo "$OUTPUT"
if [ "$STATUS" -eq 3 ] && echo "$OUTPUT" | grep -q "STARTUP_ERROR"; then
    pass "configured limits without a usable cgroup abort startup"
else
    fail "expected exit 3 + STARTUP_ERROR, got exit $STATUS"
fi

note "degradation: no limits configured, cgroup unavailable -> warn and run"
CONFIG='test:
  workers:
    - queues: "*"
      processes: 1
'
OUTPUT=$(docker run --rm --cgroupns=private \
    -e QUEBEC_ENV=test \
    -e QUEBEC_INLINE_CONFIG="$CONFIG" \
    --entrypoint /bin/sh "$IMAGE" -c \
    'mkdir -p /cfg && printf "%s" "$QUEBEC_INLINE_CONFIG" > /cfg/queue.yml
     QUEBEC_CONFIG=/cfg/queue.yml exec python3 /src/scenarios.py supervisor-boot' 2>&1)
STATUS=$?
echo "$OUTPUT"
if [ "$STATUS" -eq 0 ] && echo "$OUTPUT" | grep -q "SUPERVISOR_CONSTRUCTED"; then
    pass "no limits means a warning, not a failure"
else
    fail "expected exit 0 + SUPERVISOR_CONSTRUCTED, got exit $STATUS"
fi

note "degradation: probe reason without privileges"
docker run --rm --cgroupns=private --entrypoint python3 "$IMAGE" \
    /src/scenarios.py probe

note "rust unit tests (config + memory parsers) on linux"
docker run --rm --entrypoint cargo "$IMAGE" test --lib -- config:: memory:: 2>&1 | tail -20
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    pass "cargo test config:: memory::"
else
    fail "cargo test config:: memory::"
fi

# The wheel built inside the image, copied out for running the same build
# elsewhere (e.g. a tart VM).
note "extract wheel"
rm -rf "$REPO_ROOT/tests/docker/cgroup/wheels"
mkdir -p "$REPO_ROOT/tests/docker/cgroup/wheels"
CID=$(docker create "$IMAGE")
if docker cp "$CID:/wheels/." "$REPO_ROOT/tests/docker/cgroup/wheels/" >/dev/null; then
    docker rm -f "$CID" >/dev/null
    ls -la "$REPO_ROOT/tests/docker/cgroup/wheels/"
    pass "wheel extracted to tests/docker/cgroup/wheels/"
else
    docker rm -f "$CID" >/dev/null
    fail "wheel extraction"
fi

note "result"
if [ "$FAILURES" -eq 0 ]; then
    printf '\033[32mall checks passed\033[0m\n'
    exit 0
fi
printf '\033[31m%d check(s) failed\033[0m\n' "$FAILURES"
exit 1
