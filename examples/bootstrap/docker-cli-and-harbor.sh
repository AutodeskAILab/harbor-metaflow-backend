#!/usr/bin/env bash
# Bootstrap for images that have Python, Metaflow and boto3 but not Harbor or the
# Docker CLI. Pass it as --backend-kwarg bootstrap=<this file>; it runs on every
# Batch job before the shard's trials (see "Host preparation" in the README).
#
#   - Docker CLI, compose and buildx plugins: static binaries, pinned and checked
#     against sha256, installed under $PREFIX (inside the job container, not on the
#     host). Skipped when `docker compose` and `docker buildx` already work.
#   - Harbor: from a wheel staged next to this script (bootstrap_files=harbor-*.whl),
#     else `harbor==$HARBOR_BOOTSTRAP_VERSION` (or the latest) from the pip index.
#     Skipped when Harbor already imports and no wheel was staged. Installed with
#     --no-deps; its dependencies are installed under a constraints file frozen from
#     the image, so no package already in the image changes version (pip fails
#     instead). HARBOR_BOOTSTRAP_SKIP_DEPS lists dependencies not to install
#     (space-separated names, e.g. "litellm supabase" when the trials' agents do not
#     need them and they conflict with the image).
#
# Needs egress to download.docker.com, github.com and the pip index. Environment
# for the trials is exported through $HARBOR_METAFLOW_ENV_FILE.
set -euo pipefail

PY="${HARBOR_METAFLOW_PYTHON:-python3}"
PREFIX="${HARBOR_BOOTSTRAP_PREFIX:-/tmp/harbor-bootstrap}"
HERE="${HARBOR_METAFLOW_BOOTSTRAP_DIR:-$(pwd)}"
ENV_FILE="${HARBOR_METAFLOW_ENV_FILE:-/dev/null}"

DOCKER_URL=https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz
DOCKER_SHA256=4f798b3ee1e0140eab5bf30b0edc4e84f4cdb53255a429dc3bbae9524845d640
COMPOSE_URL=https://github.com/docker/compose/releases/download/v2.33.0/docker-compose-linux-x86_64
COMPOSE_SHA256=6395dbb256db6ea28d5c6695bc9bc33866c07ad1c93792f8d85857f1c21c34ee
BUILDX_URL=https://github.com/docker/buildx/releases/download/v0.20.1/buildx-v0.20.1.linux-amd64
BUILDX_SHA256=8c38f60308a895fa570f1410e453c5de11aafd65a99fa99965d96d24b6225a78

fetch() {  # fetch <url> <sha256> <dest>
  "$PY" - "$@" <<'PY'
import hashlib, sys, urllib.request
url, sha, dest = sys.argv[1:]
data = urllib.request.urlopen(url, timeout=300).read()
got = hashlib.sha256(data).hexdigest()
if got != sha:
    sys.exit(f"sha256 mismatch for {url}: got {got}, pinned {sha}")
open(dest, "wb").write(data)
PY
}

docker_ready() {
  command -v docker >/dev/null && docker compose version >/dev/null 2>&1 \
    && docker buildx version >/dev/null 2>&1
}

if docker_ready; then
  echo "bootstrap: docker CLI present"
else
  echo "bootstrap: installing docker CLI, compose and buildx into $PREFIX"
  mkdir -p "$PREFIX/bin" "$PREFIX/docker-config/cli-plugins"
  fetch "$DOCKER_URL" "$DOCKER_SHA256" "$PREFIX/docker.tgz"
  tar -xzf "$PREFIX/docker.tgz" -C "$PREFIX" docker/docker
  mv "$PREFIX/docker/docker" "$PREFIX/bin/docker"
  fetch "$COMPOSE_URL" "$COMPOSE_SHA256" "$PREFIX/docker-config/cli-plugins/docker-compose"
  fetch "$BUILDX_URL" "$BUILDX_SHA256" "$PREFIX/docker-config/cli-plugins/docker-buildx"
  chmod 755 "$PREFIX/bin/docker" "$PREFIX/docker-config/cli-plugins/"*
  rm -rf "$PREFIX/docker.tgz" "$PREFIX/docker"
  export PATH="$PREFIX/bin:$PATH" DOCKER_CONFIG="$PREFIX/docker-config"
  echo "PATH=$PATH" >> "$ENV_FILE"
  echo "DOCKER_CONFIG=$DOCKER_CONFIG" >> "$ENV_FILE"
fi
docker version --format 'docker client {{.Client.Version}}, server {{.Server.Version}}'

shopt -s nullglob
wheels=("$HERE"/harbor-*.whl)
if [ ${#wheels[@]} -eq 0 ] && "$PY" -c "import harbor" 2>/dev/null; then
  echo "bootstrap: harbor present"
  exit 0
fi
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
if [ ${#wheels[@]} -gt 0 ]; then
  wheel="${wheels[0]}"
else
  "$PY" -m pip download -q --no-deps -d "$tmp" "harbor${HARBOR_BOOTSTRAP_VERSION:+==$HARBOR_BOOTSTRAP_VERSION}"
  wheel="$(ls "$tmp"/harbor-*.whl)"
fi
echo "bootstrap: installing $(basename "$wheel")"
"$PY" -m pip freeze | grep -v -e '^-e ' -e ' @ ' -e '^#' > "$tmp/constraints.txt" || true
"$PY" - "$wheel" "${HARBOR_BOOTSTRAP_SKIP_DEPS:-}" > "$tmp/deps.txt" <<'PY'
import re, sys, zipfile
wheel, skip = sys.argv[1], set(sys.argv[2].lower().split())
with zipfile.ZipFile(wheel) as zf:
    meta = next(n for n in zf.namelist() if n.endswith(".dist-info/METADATA"))
    lines = zf.read(meta).decode().splitlines()
for line in lines:
    if not line.startswith("Requires-Dist:"):
        continue
    req = line.split(":", 1)[1].strip()
    if "extra ==" in req:
        continue
    name = re.split(r"[\s<>=!~;\[(]", req, maxsplit=1)[0].lower().replace("_", "-")
    if name not in skip:
        print(req)
PY
"$PY" -m pip install -q -c "$tmp/constraints.txt" -r "$tmp/deps.txt"
"$PY" -m pip install -q --no-deps --force-reinstall "$wheel"
"$PY" -c "import harbor.trial.queue, harbor.environments.factory; print('bootstrap: harbor imports')"
