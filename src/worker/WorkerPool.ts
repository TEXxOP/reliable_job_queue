import Redis from 'ioredis';
import { JobHandler } from '../queue/types';
import { RedisQueue } from '../queue/RedisQueue';
import { PostgresStorage } from '../storage/PostgresStorage';
import { RetryPolicy } from '../retry/RetryPolicy';
import { DeadLetterQueue } from '../retry/DeadLetterQueue';
import { DistributedLock } from '../lock/DistributedLock';
import { Worker } from './Worker';
import { config } from '../config';

export class WorkerPool {
  private workers: Worker[] = [];
  private redisQueue: RedisQueue;
  private storage: PostgresStorage;
  private retryPolicy: RetryPolicy;
  private dlq: DeadLetterQueue;
  private handlers: Map<string, JobHandler> = new Map();
  private redis: Redis;
  private reclaimInterval: NodeJS.Timeout | null = null;

  constructor(redis: Redis, redisQueue: RedisQueue, storage: PostgresStorage) {
    this.redis = redis;
    this.redisQueue = redisQueue;
    this.storage = storage;
    this.retryPolicy = new RetryPolicy();
    this.dlq = new DeadLetterQueue(redisQueue, storage);
  }

  // this helps ensure job types are mapped to their handler functions before workers start
  registerHandler(jobType: string, handler: JobHandler): void {
    this.handlers.set(jobType, handler);
    console.log(`Registered handler for job type: ${jobType}`);
  }

  // this helps ensure we spin up the configured number of concurrent workers
  async start(): Promise<void> {
    const concurrency = config.worker.concurrency;
    console.log(`Starting worker pool with ${concurrency} workers`);

    for (let i = 0; i < concurrency; i++) {
      const workerId = `worker-${i + 1}-${Date.now()}`;
      const lock = new DistributedLock(this.redis);
      const worker = new Worker(
        workerId,
        this.redisQueue,
        this.storage,
        this.retryPolicy,
        this.dlq,
        lock,
        this.handlers
      );

      this.workers.push(worker);

      // this helps ensure workers run concurrently without blocking each other
      worker.start().catch((err) => {
        console.error(`Worker ${workerId} crashed:`, err);
      });
    }

    // this helps ensure jobs from crashed workers are reclaimed periodically
    this.startReclaimLoop();
    this.setupGracefulShutdown();
  }

  // this helps ensure all workers finish their current jobs before the process exits
  async stop(): Promise<void> {
    console.log('Stopping worker pool...');

    if (this.reclaimInterval) {
      clearInterval(this.reclaimInterval);
    }

    await Promise.all(this.workers.map((w) => w.stop()));
    console.log('All workers stopped');
  }

  // this helps ensure the dashboard can show the health of each worker
  getWorkerStatuses(): ReturnType<Worker['getStatus']>[] {
    return this.workers.map((w) => w.getStatus());
  }

  getDLQ(): DeadLetterQueue {
    return this.dlq;
  }

  // this helps ensure jobs stuck in "processing" due to crashed workers are recovered
  private startReclaimLoop(): void {
    this.reclaimInterval = setInterval(async () => {
      try {
        const reclaimed = await this.redisQueue.reclaimTimedOutJobs();
        if (reclaimed.length > 0) {
          console.log(`Reclaimed ${reclaimed.length} timed-out jobs`);
        }
      } catch (err) {
        console.error('Reclaim loop error:', err);
      }
    }, config.worker.visibilityTimeoutSec * 1000);
  }

  // this helps ensure the process shuts down cleanly on SIGTERM/SIGINT
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      console.log(`Received ${signal}, shutting down gracefully...`);
      await this.stop();
      process.exit(0);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
  }
}
