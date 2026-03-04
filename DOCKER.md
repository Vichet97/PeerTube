# PeerTube Docker Development Setup

This docker-compose runs PostgreSQL and Redis for local PeerTube development.

## Prerequisites

- [Docker](https://www.docker.com/) and Docker Compose V2
- [Node.js](https://nodejs.org/) >= 20.x
- [pnpm](https://pnpm.io/) >= 10.9

## Quick Start

### 1. Start database services

```bash
docker compose up -d
```

This starts:
- **PostgreSQL** on `localhost:5432` (user: `peertube`, password: `peertube`, db: `peertube_dev`)
- **Redis** on `localhost:6379`

### 2. Install dependencies and build

```bash
pnpm install
pnpm run build
```

### 3. Run PeerTube

```bash
pnpm run dev
```

Or for production build:

```bash
pnpm run start
```

### 4. Access PeerTube

Open **http://localhost:9000** in your browser.

On first run, PeerTube creates an admin user. Check the logs for credentials:

```bash
docker compose logs -f  # if peertube runs in docker
# or
pnpm run dev           # logs appear in terminal
```

To reset the admin password:

```bash
pnpm run reset-password -- -u root
```

## Commands

| Command | Description |
|---------|-------------|
| `docker compose up -d` | Start PostgreSQL and Redis in background |
| `docker compose down` | Stop all services |
| `docker compose down -v` | Stop and remove volumes (resets database) |
| `docker compose logs -f postgres` | View PostgreSQL logs |

## Configuration

PeerTube uses `config/dev.yaml` when running with `NODE_ENV=dev` (default for `pnpm run dev`). The dev config already points to `127.0.0.1` for PostgreSQL and Redis, which works when using the Docker port mappings above.

### yt-dlp / Python (Import with URL)

If you get "You are using an unsupported version of Python" when importing videos by URL, set the Python path to a 3.10+ interpreter:

```bash
PEERTUBE_PYTHON_PATH=$(which python3) pnpm run dev
```

Or add to your shell profile: `export PEERTUBE_PYTHON_PATH=$(which python3)`
