#!/usr/bin/env bash
#
# Autoupdate loop for the BlockMachine validator.
#
# What it does:
#   1. Periodically fetches origin/main.
#   2. When new commits exist, pulls them and restarts the validator
#      using the same mechanism the README documents (docker compose).
#   3. Designed to survive machine reboots when wired into a system
#      service (systemd, cron @reboot, etc — see scripts/README.md).
#
# Notes:
#   - This script does NOT install itself as a service. Run it via:
#       systemd, supervisord, tmux/screen, or `nohup ./autoupdate.sh &`.
#     Examples are documented in scripts/README.md.
#   - It uses `docker compose up -d --build` (NOT `docker compose pull`)
#     because the bundled docker-compose.yml uses `build:`, not a
#     pre-built image. This matches what the README's section 5.1 does.
#   - Safe to run alongside the validator: docker compose handles
#     graceful restart of the running container.
#
set -euo pipefail

# Resolve the repo root from the script location (script lives in scripts/).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# How often to check for updates (seconds). 5 minutes is plenty —
# main doesn't move that fast and we don't want to thrash the registry.
CHECK_INTERVAL="${AUTOUPDATE_INTERVAL:-300}"

# Branch to track. Override with AUTOUPDATE_BRANCH if you maintain a fork.
BRANCH="${AUTOUPDATE_BRANCH:-main}"

log() {
    printf '%s [autoupdate] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || {
        log "ERROR: required command not found: $1"
        exit 1
    }
}

# Sanity checks: needed binaries and a git repo.
require_cmd git
require_cmd docker

if ! docker compose version >/dev/null 2>&1; then
    log "ERROR: 'docker compose' (v2 plugin) is required"
    exit 1
fi

cd "$REPO_DIR"

if [ ! -d .git ]; then
    log "ERROR: $REPO_DIR is not a git repository"
    exit 1
fi

if [ ! -f docker-compose.yml ]; then
    log "ERROR: docker-compose.yml not found in $REPO_DIR"
    exit 1
fi

restart_validator() {
    # `up -d --build` is idempotent: rebuilds the image and restarts the
    # container with the new code. Postgres data persists in the volume.
    log "Rebuilding and restarting validator..."
    if docker compose up -d --build; then
        log "Validator restarted successfully"
    else
        log "ERROR: docker compose up failed (will retry on next cycle)"
        return 1
    fi
}

# On startup: make sure the validator is running with the current checkout.
# This also covers machine reboots — when the system service kicks the
# script back up, this initial pass starts the validator if Docker hasn't
# already brought it up via `restart: unless-stopped`.
log "Starting autoupdate loop (branch=$BRANCH, interval=${CHECK_INTERVAL}s, repo=$REPO_DIR)"
log "Initial validator start..."
restart_validator || log "Initial start had errors — continuing anyway"

while true; do
    sleep "$CHECK_INTERVAL"

    # Fetch latest refs without modifying the working tree.
    if ! git fetch origin "$BRANCH" --quiet 2>&1; then
        log "git fetch failed — will retry next cycle"
        continue
    fi

    LOCAL=$(git rev-parse HEAD)
    REMOTE=$(git rev-parse "origin/$BRANCH")

    if [ "$LOCAL" = "$REMOTE" ]; then
        # Nothing to do — log at debug verbosity (uncomment for tracing).
        # log "Up to date ($LOCAL)"
        continue
    fi

    log "New commits on origin/$BRANCH ($LOCAL → $REMOTE), pulling..."

    # Hard checkout to remote — discards any local changes. Validators
    # should never be hand-editing files in this directory; if they are,
    # they shouldn't be using this script.
    if ! git reset --hard "origin/$BRANCH" --quiet; then
        log "ERROR: git reset failed — manual intervention needed"
        continue
    fi

    log "Pulled $(git rev-parse --short HEAD): $(git log -1 --pretty=format:'%s')"

    restart_validator || true
done
