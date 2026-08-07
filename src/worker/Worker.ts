import { Job, JobHandler } from '../queue/types';
import { RedisQueue } from '../queue/RedisQueue';
import { PostgresStorage } from '../storage/PostgresStorage';
import { RetryPolicy } from '../retry/RetryPolicy';
import { DeadLetterQueue } from '../retry/DeadLetterQueue';
import { DistributedLock } from '../lock/DistributedLock';
import { config } from '../config';

export class Worker {
  private id: string;
  private redisQueue: RedisQueue;
  private storage: PostgresStorage;
  private retryPolicy: RetryPolicy;
  private dlq: DeadLetterQueue;
  private lock: DistributedLock;
  private handlers: Map<string, JobHandler>;
  private running: boolean = false;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private currentJob: Job | null = null;
  public jobsProcessed: number = 0;
  public errorCount: number = 0;
  public lastHeartbeat: Date = new Date();

  constructor(
    id: string,
    redisQueue: RedisQueue,
    storage: PostgresStorage,
    retryPolicy: RetryPolicy,
    dlq: DeadLetterQueue,
    lock: DistributedLock,
    handlers: Map<string, JobHandler>
  ) {
    this.id = id;
    this.redisQueue = redisQueue;
    this.storage = storage;
    this.retryPolicy = retryPolicy;
    this.dlq = dlq;
    this.lock = lock;
    this.handlers = handlers;
  }

  // this helps ensure the worker continuously polls for new jobs until stopped
  async start(): Promise<void> {
    this.running = true;
    console.log(`Worker ${this.id} started`);

    // this helps ensure we detect stuck jobs from crashed workers
    this.startHeartbeat();

    while (this.running) {
      try {
        await this.pollAndProcess();
      } catch (err) {
        console.error(`Worker ${this.id} poll error:`, err);
        this.errorCount++;
      }

      // this helps ensure we don't hammer Redis when the queue is empty
      await this.sleep(config.worker.pollIntervalMs);
    }

    console.log(`Worker ${this.id} stopped`);
  }

  // this helps ensure the worker finishes its current job before shutting down
  async stop(): Promise<void> {
    this.running = false;
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    // this helps ensure any held locks are released on shutdown
    if (this.currentJob) {
      await this.lock.release(this.currentJob.id);
    }
  }

  private async pollAndProcess(): Promise<void> {
    const job = await this.redisQueue.dequeue(this.id);
    if (!job) return;

    // this helps ensure no two workers can process the same job simultaneously
    const acquired = await this.lock.acquire(job.id, config.worker.visibilityTimeoutSec * 1000);
    if (!acquired) {
      console.log(`Worker ${this.id} could not acquire lock for job ${job.id}, skipping`);
      return;
    }

    this.currentJob = job;

    try {
      const handler = this.handlers.get(job.type);
      if (!handler) {
        throw new Error(`No handler registered for job type: ${job.type}`);
      }

      // this helps ensure we track the job status in PostgreSQL before processing
      await this.storage.updateJobStatus(job.id, 'processing', {
        workerId: this.id,
        lockedAt: new Date(),
      });

      const startTime = Date.now();
      await handler(job);
      const duration = Date.now() - startTime;

      // this helps ensure completed jobs are properly acknowledged in both Redis and PostgreSQL
      await this.redisQueue.ack(job.id);
      await this.storage.updateJobStatus(job.id, 'completed', {
        completedAt: new Date(),
      });

      this.jobsProcessed++;
      console.log(`Worker ${this.id} completed job ${job.id} (${job.type}) in ${duration}ms`);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      console.error(`Worker ${this.id} failed job ${job.id}: ${error}`);
      this.errorCount++;

      await this.handleFailure(job, error);
    } finally {
      await this.lock.release(job.id);
      this.currentJob = null;
    }
  }

  // this helps ensure failed jobs are either retried with backoff or moved to the DLQ
  private async handleFailure(job: Job, error: string): Promise<void> {
    const updatedJob = { ...job, attempts: job.attempts + 1 };

    if (this.retryPolicy.shouldRetry(updatedJob)) {
      const delay = this.retryPolicy.calculateDelay(updatedJob.attempts);
      await this.redisQueue.nack(job.id, error, delay);
      await this.storage.updateJobStatus(job.id, 'pending', {
        attempts: updatedJob.attempts,
        error,
      });
      console.log(`Job ${job.id} scheduled for retry #${updatedJob.attempts} in ${delay}ms`);
    } else {
      // this helps ensure jobs that have exhausted all retries go to the dead letter queue
      await this.dlq.moveToDeadLetter(updatedJob, error);
      console.log(`Job ${job.id} moved to DLQ after ${updatedJob.attempts} attempts`);
    }
  }

  // this helps ensure the worker's visibility timeout is extended while processing long jobs
  private startHeartbeat(): void {
    this.heartbeatInterval = setInterval(async () => {
      this.lastHeartbeat = new Date();
      if (this.currentJob) {
        await this.redisQueue.extendVisibility(
          this.currentJob.id,
          config.worker.visibilityTimeoutSec
        );
        await this.lock.extend(
          this.currentJob.id,
          config.worker.visibilityTimeoutSec * 1000
        );
      }
    }, (config.worker.visibilityTimeoutSec / 2) * 1000);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  getId(): string {
    return this.id;
  }

  isRunning(): boolean {
    return this.running;
  }

  getStatus(): { id: string; running: boolean; jobsProcessed: number; errorCount: number; lastHeartbeat: Date } {
    return {
      id: this.id,
      running: this.running,
      jobsProcessed: this.jobsProcessed,
      errorCount: this.errorCount,
      lastHeartbeat: this.lastHeartbeat,
    };
  }
}
