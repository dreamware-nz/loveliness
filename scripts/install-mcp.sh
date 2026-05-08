#!/usr/bin/env sh
# install-mcp.sh — install loveliness-mcp and wire it into Claude.
#
# Usage:
#   ./scripts/install-mcp.sh                # install + register MCP + skill
#   ./scripts/install-mcp.sh --no-skill     # skip skill install
#   ./scripts/install-mcp.sh --no-register  # build + install only
#   LOVELINESS_URL=http://prod:8080 ./scripts/install-mcp.sh
#
# Env:
#   LOVELINESS_URL    HTTP endpoint of a Loveliness node (default http://localhost:8080)
#   LOVELINESS_TOKEN  Bearer token for an auth-enabled cluster (optional)
#   PREFIX            Where to install loveliness-mcp (default ${GOBIN:-$(go env GOPATH)/bin})
#
# Exit codes: 0 on success, 1 on failure, 2 on bad usage.

set -eu

want_skill=1
want_register=1

for arg; do
  case "$arg" in
    --no-skill)    want_skill=0 ;;
    --no-register) want_register=0 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
    *)
      printf 'unknown flag: %s\n' "$arg" >&2
      exit 2
      ;;
  esac
done

repo_root() {
  cd "$(dirname "$0")/.." && pwd
}

REPO="$(repo_root)"
PREFIX="${PREFIX:-${GOBIN:-$(go env GOPATH 2>/dev/null || printf '')/bin}}"
LOVELINESS_URL="${LOVELINESS_URL:-http://localhost:8080}"

if ! command -v go >/dev/null 2>&1; then
  printf 'error: go toolchain not found on PATH\n' >&2
  exit 1
fi

if [ -z "${PREFIX:-}" ] || [ "$PREFIX" = "/bin" ]; then
  printf 'error: could not resolve install prefix; set PREFIX or GOPATH\n' >&2
  exit 1
fi

mkdir -p "$PREFIX"

printf '==> building loveliness-mcp\n'
( cd "$REPO" && CGO_ENABLED=0 go build -o "$PREFIX/loveliness-mcp" ./cmd/loveliness-mcp )
printf '    installed: %s/loveliness-mcp\n' "$PREFIX"

if [ "$want_register" = 1 ]; then
  if command -v claude >/dev/null 2>&1; then
    printf '==> registering MCP server with Claude\n'
    # `claude mcp add` is idempotent in recent CLI versions; if it isn't,
    # remove first to avoid a duplicate-name error.
    claude mcp remove loveliness >/dev/null 2>&1 || true
    if [ -n "${LOVELINESS_TOKEN:-}" ]; then
      claude mcp add loveliness \
        --env "LOVELINESS_URL=$LOVELINESS_URL" \
        --env "LOVELINESS_TOKEN=$LOVELINESS_TOKEN" \
        -- "$PREFIX/loveliness-mcp"
    else
      claude mcp add loveliness \
        --env "LOVELINESS_URL=$LOVELINESS_URL" \
        -- "$PREFIX/loveliness-mcp"
    fi
    printf '    registered as `loveliness` (URL=%s)\n' "$LOVELINESS_URL"
  else
    cat <<EOF
==> claude CLI not found; skipping auto-register
    Add this to your MCP client config manually:

    {
      "mcpServers": {
        "loveliness": {
          "command": "$PREFIX/loveliness-mcp",
          "env": { "LOVELINESS_URL": "$LOVELINESS_URL" }
        }
      }
    }
EOF
  fi
else
  printf '==> skipping MCP registration (--no-register)\n'
fi

if [ "$want_skill" = 1 ]; then
  SKILL_SRC="$REPO/skills/loveliness"
  SKILL_DST="${HOME}/.claude/skills/loveliness"
  if [ ! -d "$SKILL_SRC" ]; then
    printf '==> skill source missing at %s; skipping\n' "$SKILL_SRC"
  else
    printf '==> installing skill to %s\n' "$SKILL_DST"
    mkdir -p "$(dirname "$SKILL_DST")"
    rm -rf "$SKILL_DST"
    # Symlink rather than copy so updates from `git pull` flow through.
    ln -s "$SKILL_SRC" "$SKILL_DST"
    printf '    linked: %s -> %s\n' "$SKILL_DST" "$SKILL_SRC"
  fi
else
  printf '==> skipping skill install (--no-skill)\n'
fi

printf '\nDone. Restart your MCP client (e.g. Claude Code) to pick up the new server.\n'
