# Validator scripts

## `autoupdate.sh`

Background loop that pulls `origin/main` periodically and rebuilds the
validator container when new commits land. Uses the same `docker compose`
mechanism the main README documents (section 5.1) so it works for any
validator following the standard setup.

### What it does

- Polls `origin/main` every 5 minutes (configurable via `AUTOUPDATE_INTERVAL`)
- On new commits: `git reset --hard origin/main` then `docker compose up -d --build`
- Logs to stdout — pipe to a log file or rely on systemd/journald

### Run it manually (foreground, useful for testing)

```bash
./scripts/autoupdate.sh
```

### Run it as a persistent service

You need to keep this script running across machine reboots. Pick whichever
fits your setup.

#### systemd (recommended on Linux)

Create `/etc/systemd/system/blockmachine-autoupdate.service`:

```ini
[Unit]
Description=BlockMachine validator autoupdate
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/path/to/blockmachine-validator
ExecStart=/path/to/blockmachine-validator/scripts/autoupdate.sh
Restart=on-failure
RestartSec=30
User=YOUR_USER
# Environment overrides (optional):
# Environment=AUTOUPDATE_INTERVAL=300
# Environment=AUTOUPDATE_BRANCH=main

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now blockmachine-autoupdate
sudo journalctl -u blockmachine-autoupdate -f
```

#### cron (simpler, less robust)

```bash
crontab -e
# Add:
@reboot cd /path/to/blockmachine-validator && nohup ./scripts/autoupdate.sh >> /var/log/bm-autoupdate.log 2>&1 &
```

You'll also want to start it the first time without a reboot:

```bash
nohup ./scripts/autoupdate.sh >> /var/log/bm-autoupdate.log 2>&1 &
```

#### tmux / screen (interactive, simplest)

```bash
tmux new -d -s autoupdate './scripts/autoupdate.sh'
# Detach: Ctrl-b d. Reattach: tmux attach -t autoupdate
```

This won't survive reboots — combine with `@reboot` cron if needed.

### Environment variables

| Variable | Default | What it does |
|---|---|---|
| `AUTOUPDATE_INTERVAL` | 300 | Seconds between update checks |
| `AUTOUPDATE_BRANCH` | main | Branch to track (override for forks) |

### Important notes

- The script does `git reset --hard origin/main`. **Don't hand-edit files
  in the repo while the autoupdater is running** — your changes will be
  discarded on the next pull.
- Postgres data is preserved across rebuilds (it's in a Docker volume).
- The first thing the script does on startup is run `docker compose up -d --build`.
  After a machine reboot, the validator container will already be running
  via `restart: unless-stopped`, but this initial pass also picks up any
  commits that landed while the machine was offline.
- If a build fails, the previous container keeps running and the script
  retries on the next cycle.
