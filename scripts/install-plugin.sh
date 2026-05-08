#!/usr/bin/env sh
# install-plugin.sh — install loveliness as a Claude Code plugin.
#
# Builds loveliness-mcp into $GOBIN, registers this repo as a Claude
# plugin marketplace, and installs the plugin at user scope so the MCP
# server + skill are visible from every Claude Code project.
#
# Usage:
#   ./scripts/install-plugin.sh              # install marketplace + plugin
#   ./scripts/install-plugin.sh --no-build   # skip building loveliness-mcp
#   ./scripts/install-plugin.sh --update     # update an already-installed marketplace
#
# Env:
#   PREFIX  Where to install loveliness-mcp (default ${GOBIN:-$(go env GOPATH)/bin})
#
# Exit codes: 0 on success, 1 on failure, 2 on bad usage.

set -eu

want_build=1
mode=add

for arg; do
  case "$arg" in
    --no-build) want_build=0 ;;
    --update)   mode=update ;;
    -h|--help)
      sed -n '2,15p' "$0"
      exit 0
      ;;
    *)
      printf 'unknown flag: %s\n' "$arg" >&2
      exit 2
      ;;
  esac
done

repo_root() { cd "$(dirname "$0")/.." && pwd; }
REPO="$(repo_root)"

if ! command -v claude >/dev/null 2>&1; then
  printf 'error: claude CLI not found on PATH — install Claude Code first\n' >&2
  exit 1
fi

if [ "$want_build" = 1 ]; then
  command -v go >/dev/null 2>&1 \
    || { printf 'error: go toolchain not found on PATH\n' >&2; exit 1; }

  PREFIX="${PREFIX:-${GOBIN:-$(go env GOPATH 2>/dev/null || printf '')/bin}}"
  if [ -z "${PREFIX:-}" ] || [ "$PREFIX" = "/bin" ]; then
    printf 'error: could not resolve install prefix; set PREFIX or GOPATH\n' >&2
    exit 1
  fi

  mkdir -p "$PREFIX"
  printf '==> building loveliness-mcp\n'
  ( cd "$REPO" && CGO_ENABLED=0 go build -o "$PREFIX/loveliness-mcp" ./cmd/loveliness-mcp )
  printf '    installed: %s/loveliness-mcp\n' "$PREFIX"

  case ":$PATH:" in
    *":$PREFIX:"*) ;;
    *) printf '\nNOTE: %s is not on PATH. Add to your shell rc:\n  export PATH="%s:$PATH"\n\n' "$PREFIX" "$PREFIX" ;;
  esac
fi

# Drop any standalone user-scope MCP registration so the plugin one isn't
# shadowed or duplicated. Safe to ignore failures.
claude mcp remove loveliness --scope user  >/dev/null 2>&1 || true
claude mcp remove loveliness --scope local >/dev/null 2>&1 || true

if [ "$mode" = update ]; then
  printf '==> updating marketplace `loveliness`\n'
  claude plugin marketplace update loveliness
else
  # `claude plugin marketplace add` errors if the marketplace name is already
  # registered, so try update first when adding from this repo path.
  if claude plugin marketplace list 2>/dev/null | grep -q '^[[:space:]]*❯[[:space:]]*loveliness$'; then
    printf '==> marketplace `loveliness` already registered; updating\n'
    claude plugin marketplace update loveliness
  else
    printf '==> registering marketplace `loveliness` from %s\n' "$REPO"
    claude plugin marketplace add "$REPO"
  fi
fi

printf '==> installing plugin loveliness@loveliness (scope=user)\n'
claude plugin install loveliness@loveliness --scope user

cat <<EOF

Done. Restart your MCP client (e.g. Claude Code) so it picks up the
plugin's MCP server. The \`loveliness-mcp\` binary must be on PATH.
EOF
