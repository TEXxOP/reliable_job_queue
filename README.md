# Reliable Job Queue with Dead Letter Recovery

A fault-tolerant distributed job queue handling **100K+ background jobs/day** with durable storage, visibility timeouts, and worker crash recovery. Built with **TypeScript**, **Node.js**, **Redis**, **PostgreSQL**, and **Docker**.

## Architecture

```
                         ┌─────────────────────────────────────┐
                         │           Producers (REST API)       │
                         │         POST /api/jobs               │
                         └──────────────┬──────────────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────────────┐
                         │              Redis                   │
                         │                                      │
                         │  ┌───────────┐   ┌───────────────┐  │
                         │  │  Pending   │   │  Processing   │  │
                         │  │ Sorted Set │──▶│  Sorted Set   │  │
                         │  │ (priority) │   │  (vis. timeout)│  │
                         │  └───────────┘   └───────────────┘  │
                         │                                      │
                         │  ┌───────────────────────────────┐  │
                         │  │  Distributed Locks (SET NX PX) │  │
                         │  └───────────────────────────────┘  │
                         └──────────────┬──────────────────────┘
                                        │
                          ┌─────────────┼─────────────┐
                          ▼             ▼             ▼
                    ┌──────────┐  ┌──────────┐  ┌──────────┐
                    │ Worker 1 │  │ Worker 2 │  │ Worker N │
                    │          │  │          │  │          │
                    │ • Poll   │  │ • Poll   │  │ • Poll   │
                    │ • Lock   │  │ • Lock   │  │ • Lock   │
                    │ • Execute│  │ • Execute│  │ • Execute│
                    │ • Ack    │  │ • Ack    │  │ • Ack    │
                    └────┬─────┘  └────┬─────┘  └────┬─────┘
                         │             │             │
                         └─────────────┼─────────────┘
                                       ▼
                         ┌─────────────────────────────────────┐
                         │            PostgreSQL                │
                         │                                      │
                         │  ┌──────────────┐  ┌─────────────┐  │
                         │  │  jobs table   │  │  dead_letter │  │
                         │  │  (durable)    │  │  _jobs table │  │
                         │  └──────────────┘  └─────────────┘  │
                         └──────────────┬──────────────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────────────┐
                         │       Observability Dashboard        │
                         │                                      │
                         │  • Real-time metrics via WebSocket   │
                         │  • Throughput, retry rates, health   │
                         │  • DLQ inspection and recovery       │
                         └─────────────────────────────────────┘
```

## How It Works

### Job Lifecycle

1. **Enqueue** — A producer sends a job via the REST API. The job is written to both Redis (for fast processing) and PostgreSQL (for durability).

2. **Dequeue** — Workers atomically pop the highest-priority job from Redis using a Lua script. The job becomes invisible to other workers for a configurable timeout.

3. **Process** — The worker acquires a distributed lock, executes the job handler, and sends heartbeats to extend the visibility timeout.

4. **Ack / Nack** — On success, the job is acknowledged and marked completed. On failure, it's re-enqueued with an exponential backoff delay.

5. **Dead Letter** — After exhausting all retry attempts, the job moves to the Dead Letter Queue in PostgreSQL where it can be inspected and recovered.

### Visibility Timeouts

When a worker picks up a job, it becomes invisible to other workers for 30 seconds (configurable). This prevents duplicate processing. If the worker crashes, the timeout expires and the job becomes visible again for another worker to pick up.

### Retry with Exponential Backoff

Failed jobs are retried with increasing delays: `baseDelay × 2^attempt + random jitter`. The jitter prevents thundering herd problems when multiple jobs fail simultaneously.

```
Attempt 1: ~1s delay
Attempt 2: ~2s delay
Attempt 3: ~4s delay
Attempt 4: ~8s delay
Attempt 5: moved to DLQ
```

### Distributed Locking

Workers use Redis `SET NX PX` to acquire locks before processing. Lua scripts ensure only the lock owner can release or extend the lock, preventing one worker from accidentally interfering with another.

### Dead Letter Queue Recovery

Jobs that fail permanently are stored in a PostgreSQL `dead_letter_jobs` table. The dashboard provides one-click recovery, which re-enqueues the job with fresh retry attempts.

## Tech Stack

| Component | Technology | Purpose |
|:---|:---|:---|
| Queue Engine | Redis 7 | Fast in-memory job queue with sorted sets for priority ordering |
| Durable Storage | PostgreSQL 15 | Persistent job storage, metrics, and DLQ |
| Runtime | Node.js + TypeScript | Type-safe application logic |
| API | Express.js | REST API for job producers |
| Dashboard | Express + WebSocket | Real-time observability UI |
| Containerization | Docker + Compose | One-command deployment |

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Node.js 20+ (for local development)

### Run with Docker

```bash
# Clone the repo
git clone https://github.com/TEXxOP/reliable_job_queue.git
cd reliable_job_queue

# Start all services (Redis, PostgreSQL, API, Worker, Dashboard)
docker-compose up --build

# API available at    http://localhost:3000
# Dashboard at        http://localhost:3001
```

### Run Locally

```bash
# Install dependencies
npm install

# Copy environment config
cp .env.example .env

# Start Redis and PostgreSQL (via Docker)
docker-compose up redis postgres -d

# In separate terminals:
npm run dev         # API server (port 3000)
npm run worker      # Worker pool
npm run dashboard   # Dashboard (port 3001)
```

### Seed Test Jobs

```bash
# Seed 15 jobs (default — mix of email, payment, report with varied priorities)
npm run seed

# Seed a custom number of jobs
npm run seed -- 50

# Seed 100 jobs to stress-test the workers
npm run seed -- 100
```

Watch them get processed in real-time on the dashboard: http://localhost:3001

## API Reference

### Enqueue a Job

```bash
curl -X POST http://localhost:3000/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email",
    "payload": { "to": "user@example.com", "subject": "Welcome!", "body": "Hello!" },
    "priority": "high"
  }'
```

### Check Job Status

```bash
curl http://localhost:3000/api/jobs/<job-id>
```

### View Queue Stats

```bash
curl http://localhost:3000/api/stats
```

### Cancel a Pending Job

```bash
curl -X DELETE http://localhost:3000/api/jobs/<job-id>
```

### Health Check

```bash
curl http://localhost:3000/api/health
```

## Configuration

All configuration is via environment variables (see `.env.example`):

| Variable | Default | Description |
|:---|:---|:---|
| `REDIS_HOST` | localhost | Redis server host |
| `REDIS_PORT` | 6379 | Redis server port |
| `POSTGRES_HOST` | localhost | PostgreSQL host |
| `POSTGRES_PORT` | 5432 | PostgreSQL port |
| `POSTGRES_DB` | jobqueue | Database name |
| `API_PORT` | 3000 | REST API port |
| `DASHBOARD_PORT` | 3001 | Dashboard port |
| `WORKER_CONCURRENCY` | 5 | Number of concurrent workers |
| `VISIBILITY_TIMEOUT_SEC` | 30 | Seconds before a processing job becomes visible again |
| `MAX_RETRY_ATTEMPTS` | 5 | Max retries before moving to DLQ |
| `RETRY_BASE_DELAY_MS` | 1000 | Base delay for exponential backoff |

## Project Structure

```
src/
├── api/
│   └── server.ts              # REST API for producers
├── config.ts                   # Centralized configuration
├── dashboard/
│   ├── server.ts              # Dashboard Express + WebSocket server
│   └── public/
│       ├── index.html         # Dashboard UI
│       └── styles.css         # Dark theme styles
├── index.ts                   # API entry point
├── lock/
│   └── DistributedLock.ts     # Redis distributed locking
├── queue/
│   ├── RedisQueue.ts          # Redis queue engine
│   ├── scripts/
│   │   └── dequeue.lua        # Atomic dequeue Lua script
│   └── types.ts               # Type definitions
├── retry/
│   ├── DeadLetterQueue.ts     # DLQ management and recovery
│   └── RetryPolicy.ts         # Exponential backoff with jitter
├── storage/
│   ├── PostgresStorage.ts     # PostgreSQL persistence layer
│   └── migrations/
│       ├── 001_create_jobs_table.sql
│       └── 002_create_dlq_table.sql
├── worker.ts                  # Worker entry point
└── worker/
    ├── Worker.ts              # Individual worker with heartbeat
    ├── WorkerPool.ts          # Concurrent worker manager
    └── handlers.ts            # Example job handlers
```

## License

MIT
