# BlockMachine Validator Guide

## Overview

BlockMachine (Subnet 19) is a decentralized marketplace for blockchain RPC infrastructure. Validators independently audit miner performance by reading gateway logs from public storage, verifying response correctness against reference nodes, computing quality-weighted scores, and submitting weights on-chain each epoch (361 blocks, ~72 minutes).

---

## 1. Requirements

**Hardware**

- 4+ CPU cores
- 8 GB RAM (16 GB recommended)
- 50 GB SSD
- Stable internet connection

**Software**

- Python 3.11+ (3.13 recommended)
- Docker and Docker Compose (if running containerized)
- PostgreSQL 15+ (optional — the validator can run without a database)

**Bittensor**

- A registered hotkey on subnet 19 (finney) or subnet 417 (testnet)
- Enough TAO staked to meet the subnet's minimum stake threshold

---

## 2. Wallet Setup

If you already have a Bittensor wallet registered on the subnet, skip ahead.

### 2.1 Install btcli

```bash
pip install bittensor-cli
```

### 2.2 Create a wallet

```bash
btcli wallet new-coldkey --wallet-name validator
btcli wallet new-hotkey  --wallet-name validator --hotkey-name default
btcli wallet list
```

### 2.3 Register on the subnet

**Mainnet (subnet 19):**

```bash
btcli subnet register --wallet-name validator --hotkey-name default --netuid 19
```

**Testnet (subnet 417):**

```bash
btcli subnet register --wallet-name validator --hotkey-name default --netuid 417 --network test
```

---

## 3. Configuration

The validator fetches network-wide settings (epoch length, scoring, reference nodes, verification gateway URL, S3 log location, CU schedule, …) from the registry at startup. You only set identity and local infrastructure via env vars.

### 3.1 Environment variables

| Variable | What it controls | Default |
|---|---|---|
| `NETUID` | Subnet UID (19 for mainnet, 417 for testnet) | — |
| `SUBTENSOR_NETWORK` | Bittensor network (`finney` or `test`) | finney |
| `REGISTRY_URL` | **Required.** Registry base URL | — |
| `GATEWAY_AUTH_URL` | Authentication endpoint for challenge-response flow | `https://test-auth.taostats.io` (testnet) |
| `GATEWAY_CLIENT_ID` | OAuth client ID for gateway auth | `07f5c729-5ca7-412a-b5e7-4966e132548e` |
| `WALLET_NAME` | Bittensor wallet directory | validator |
| `WALLET_HOTKEY` | Hotkey name within wallet | default |
| `WALLET_HOTKEY_SEED` | Opaque hotkey seed string from the wallet file, for headless containers without a mounted `~/.bittensor` | — |
| `DB_ENABLED` | Enable local PostgreSQL audit store | true |
| `LOCAL_DB_HOST` | Postgres host | localhost |
| `LOCAL_DB_PORT` | Postgres port | 5432 |
| `LOCAL_DB_NAME` | Postgres database name | blockmachine_validator |
| `LOCAL_DB_USER` | Postgres user | validator |
| `LOCAL_DB_PASSWORD` | Postgres password | — |
| `DB_RETENTION_DAYS` | Days to keep audit rows | 30 |
| `METRICS_PORT` | Prometheus metrics port | 9090 |

Everything else — reference nodes, burn sink, S3 bucket, verification gateway — is served by the registry and automatically applied at startup. The validator signs a challenge with its hotkey to authenticate against the registry; no manual tokens to manage.

### 3.2 Example env files

Two templates are provided: [.env.example.mainnet](.env.example.mainnet) and [.env.example.testnet](.env.example.testnet). Copy the one you need to `.env` and fill in `LOCAL_DB_PASSWORD`.

```bash
cp .env.example.mainnet .env     # or .env.example.testnet
```

---

## 4. Database Setup (optional)

Set `DB_ENABLED=false` to run without Postgres. Epoch audit data won't be persisted but the validator will work.

For production, create a dedicated database:

```bash
sudo -u postgres psql <<SQL
CREATE USER validator WITH PASSWORD 'your-password-here';
CREATE DATABASE blockmachine_validator OWNER validator;
SQL
```

Tables are created automatically on first connection.

---

## 5. Running the Validator

### 5.1 Docker Compose

A [docker-compose.yml](docker-compose.yml) is included that starts Postgres + the validator. Copy one of the `.env.example.*` files to `.env`, fill in `LOCAL_DB_PASSWORD`, then:

```bash
docker compose up -d
docker compose logs -f validator
```

### 5.2 Running directly

```bash
pip install -r requirements.txt
set -a && source .env && set +a
python -m validator.main
```

### 5.3 Headless / no mounted wallet

If you can't mount `~/.bittensor`, set `WALLET_HOTKEY_SEED` to the seed string found in your hotkey file (the `secretSeed` field inside `~/.bittensor/wallets/<wallet>/hotkeys/<hotkey>`). Keep this secret — anyone with it can sign as your hotkey.

---

## 6. Verifying It Works

Watch the startup logs for:

```
INFO - Starting BlockMachine Validator (netuid=19, network=finney)
INFO - BittensorChain initialized: network=finney, netuid=19, hotkey=5Exx...
INFO - Fetched validator config from registry
INFO - Database connected and schema initialized
INFO - Validator initialized — starting weight loop + verification loop
```

Within a few minutes the weight loop should pick up an epoch and submit weights:

```
INFO - Processing epoch 7668894 for weights
INFO - Metagraph synced: 12 miners
INFO - Alpha price: $2.9260
INFO - Weights submitted successfully
INFO - Epoch 7668894 done — paid 4 miners, burn=95.2%, submitted=True
```

And the verification loop should be checking sampled queries:

```
INFO - [verification] processing epoch 7668894
INFO - Verification PASS: state_getStorage (chain=TAO)
INFO - [verification] epoch 7668894 complete
```

### Warning signs

| Log message | Meaning | Action |
|---|---|---|
| `Failed to fetch validator config from registry` | Registry unreachable at startup — falling back to defaults | Check `REGISTRY_URL` and network connectivity |
| `set_weights rejected` | Chain rejected the submission | Check stake, registration, rate limits |
| `No CU allocations for epoch X` | No gateway traffic for that epoch | Normal for quiet periods; the validator will burn after a retry window |
| `Verification FAIL` | A miner returned incorrect data | Automatic — miner gets banned |
| `Token expired, attempting refresh` | Gateway auth token expired | Automatic — the validator re-runs the hotkey challenge flow |

---

## 7. Updating

```bash
docker compose pull
docker compose up -d
```

---

## 8. FAQ

**Can I run without a database?**
Yes. Set `DB_ENABLED=false`. Epoch audit history won't be persisted across restarts.

**How do I switch between testnet and mainnet?**
Use the corresponding `.env.example.*` file. `NETUID`, `SUBTENSOR_NETWORK`, `REGISTRY_URL`, and `GATEWAY_AUTH_URL` are the variables that change between networks.

**What happens if my validator goes down?**
You miss weight submissions for the epochs you're offline. Prolonged downtime reduces your validator's effective influence.

**What is the burn sink?**
A UID (set by the registry) that receives the portion of emissions miners didn't earn through actual work. If miners consumed $50 of a $500 emission pool, the remaining $450 goes to burn.

---

## 9. Architecture

```
1. Gateway routes customer requests to miners, writes logs to S3
2. Epoch finalizer detects completion, writes miner-configs.json
3. Validator reads logs + configs from S3
4. Verification loop samples queries, re-executes against reference nodes
5. Weight loop computes weights from CU × price, applies blacklist
6. Weights submitted on-chain
7. Bittensor consensus aggregates validator weights → emissions distributed
```

**Weight formula:** each miner's weight is proportional to `CU_served × target_usd_per_cu`, bounded by 41% of total subnet emissions. If total miner asks exceed the pool, payouts scale down proportionally. Unearned emissions go to the burn sink.

**Verification:** a confirmed hash mismatch (miner response ≠ reference response at the same block) results in a permanent coldkey ban.

---

## Support

- GitHub: https://github.com/taostat/blockmachine
- Discord: Subnet 19 channels in the Bittensor Discord
