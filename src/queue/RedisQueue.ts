import Redis from 'ioredis';
import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { Job, CreateJobInput, PRIORITY_SCORES, QueueConfig } from './types';
import { config } from '../config';

// this helps ensure all Redis key names are in one place to avoid typos
const KEYS = {
  pending: 'queue:pending',
  processing: 'queue:processing',
  jobPrefix: 'queue:job:',
  completedCount: 'queue:stats:completed',
  failedCount: 'queue:stats:failed',
};

export class RedisQueue {
  private redis: Redis;
  private dequeueScript: string;
  private queueConfig: QueueConfig;

  constructor(redis: Redis) {
    this.redis = redis;
    this.queueConfig = {
      visibilityTimeoutSec: config.worker.visibilityTimeoutSec,
      pollIntervalMs: config.worker.pollIntervalMs,
    };

    // this helps ensure the Lua script is loaded once at startup instead of every dequeue call
    this.dequeueScript = fs.readFileSync(
      path.join(__dirname, 'scripts', 'dequeue.lua'),
      'utf-8'
    );
  }

  // this helps ensure each job gets a unique id and is stored with all its metadata
  async enqueue(input: CreateJobInput): Promise<Job> {
    const job: Job = {
      id: uuidv4(),
      type: input.type,
      payload: input.payload,
      priority: input.priority || 'normal',
      status: 'pending',
      attempts: 0,
      maxAttempts: input.maxAttempts || config.retry.maxAttempts,
      createdAt: new Date(),
      updatedAt: new Date(),
      scheduledFor: input.scheduledFor || new Date(),
      lockedAt: null,
      completedAt: null,
      error: null,
      workerId: null,
    };

    // this helps ensure we store job data in a Redis hash for quick field-level access
    const jobKey = KEYS.jobPrefix + job.id;
    await this.redis.hmset(jobKey, this.serializeJob(job));

    // this helps ensure priority ordering: lower score = higher priority
    // we combine priority score with timestamp so same-priority jobs are FIFO
    const score = PRIORITY_SCORES[job.priority] * 1e10 + job.scheduledFor.getTime();
    await this.redis.zadd(KEYS.pending, score, job.id);

    return job;
  }

  // this helps ensure only one worker grabs each job using an atomic Lua script
  async dequeue(workerId: string): Promise<Job | null> {
    const now = Math.floor(Date.now() / 1000);
    const result = await this.redis.eval(
      this.dequeueScript,
      3,
      KEYS.pending,
      KEYS.processing,
      KEYS.jobPrefix,
      now.toString(),
      this.queueConfig.visibilityTimeoutSec.toString(),
      workerId
    ) as string[] | null;

    if (!result || result.length === 0) {
      return null;
    }

    return this.parseHashResult(result);
  }

  // this helps ensure completed jobs are removed from the processing set
  async ack(jobId: string): Promise<void> {
    const jobKey = KEYS.jobPrefix + jobId;

    await this.redis.zrem(KEYS.processing, jobId);
    await this.redis.hmset(jobKey, {
      status: 'completed',
      completedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    await this.redis.incr(KEYS.completedCount);
  }

  // this helps ensure failed jobs go back to the pending set for retry
  async nack(jobId: string, error: string, nextAttemptDelay: number): Promise<void> {
    const jobKey = KEYS.jobPrefix + jobId;

    await this.redis.zrem(KEYS.processing, jobId);

    const attemptsStr = await this.redis.hget(jobKey, 'attempts');
    const attempts = parseInt(attemptsStr || '0', 10) + 1;

    await this.redis.hmset(jobKey, {
      status: 'pending',
      attempts: attempts.toString(),
      error: error,
      updatedAt: new Date().toISOString(),
      lockedAt: '',
      workerId: '',
    });

    // this helps ensure retried jobs are scheduled in the future based on backoff delay
    const retryScore = Date.now() + nextAttemptDelay;
    await this.redis.zadd(KEYS.pending, retryScore, jobId);
  }

  // this helps ensure jobs that have permanently failed are removed from the active queue
  async moveToDead(jobId: string, reason: string): Promise<void> {
    const jobKey = KEYS.jobPrefix + jobId;

    await this.redis.zrem(KEYS.processing, jobId);
    await this.redis.hmset(jobKey, {
      status: 'dead',
      error: reason,
      updatedAt: new Date().toISOString(),
    });
    await this.redis.incr(KEYS.failedCount);
  }

  // this helps ensure long-running jobs don't get re-queued while still being processed
  async extendVisibility(jobId: string, extraSeconds: number): Promise<void> {
    const newDeadline = Math.floor(Date.now() / 1000) + extraSeconds;
    await this.redis.zadd(KEYS.processing, newDeadline, jobId);
  }

  // this helps ensure crashed workers' jobs get reclaimed after the visibility timeout expires
  async reclaimTimedOutJobs(): Promise<string[]> {
    const now = Math.floor(Date.now() / 1000);

    // find all jobs in the processing set whose visibility timeout has expired
    const expiredJobIds = await this.redis.zrangebyscore(KEYS.processing, '-inf', now);

    for (const jobId of expiredJobIds) {
      const jobKey = KEYS.jobPrefix + jobId;

      await this.redis.zrem(KEYS.processing, jobId);
      await this.redis.hmset(jobKey, {
        status: 'pending',
        updatedAt: new Date().toISOString(),
        lockedAt: '',
        workerId: '',
      });

      // this helps ensure reclaimed jobs get high priority so they aren't delayed further
      const priorityStr = await this.redis.hget(jobKey, 'priority') || 'normal';
      const priorityScore = PRIORITY_SCORES[priorityStr as keyof typeof PRIORITY_SCORES] || 20;
      const score = priorityScore * 1e10 + Date.now();
      await this.redis.zadd(KEYS.pending, score, jobId);
    }

    return expiredJobIds;
  }

  // this helps ensure we can check how many jobs are in each state
  async getQueueDepth(): Promise<{ pending: number; processing: number }> {
    const [pending, processing] = await Promise.all([
      this.redis.zcard(KEYS.pending),
      this.redis.zcard(KEYS.processing),
    ]);
    return { pending, processing };
  }

  // this helps ensure we can look up any job by its id
  async getJob(jobId: string): Promise<Job | null> {
    const jobKey = KEYS.jobPrefix + jobId;
    const data = await this.redis.hgetall(jobKey);
    if (!data || Object.keys(data).length === 0) return null;
    return this.deserializeJob(data);
  }

  async getCompletedCount(): Promise<number> {
    const count = await this.redis.get(KEYS.completedCount);
    return parseInt(count || '0', 10);
  }

  async getFailedCount(): Promise<number> {
    const count = await this.redis.get(KEYS.failedCount);
    return parseInt(count || '0', 10);
  }

  // this helps ensure job data is stored as flat strings in Redis hashes
  private serializeJob(job: Job): Record<string, string> {
    return {
      id: job.id,
      type: job.type,
      payload: JSON.stringify(job.payload),
      priority: job.priority,
      status: job.status,
      attempts: job.attempts.toString(),
      maxAttempts: job.maxAttempts.toString(),
      createdAt: job.createdAt.toISOString(),
      updatedAt: job.updatedAt.toISOString(),
      scheduledFor: job.scheduledFor.toISOString(),
      lockedAt: job.lockedAt ? job.lockedAt.toISOString() : '',
      completedAt: job.completedAt ? job.completedAt.toISOString() : '',
      error: job.error || '',
      workerId: job.workerId || '',
    };
  }

  private deserializeJob(data: Record<string, string>): Job {
    return {
      id: data.id,
      type: data.type,
      payload: JSON.parse(data.payload || '{}'),
      priority: data.priority as Job['priority'],
      status: data.status as Job['status'],
      attempts: parseInt(data.attempts || '0', 10),
      maxAttempts: parseInt(data.maxAttempts || '5', 10),
      createdAt: new Date(data.createdAt),
      updatedAt: new Date(data.updatedAt),
      scheduledFor: new Date(data.scheduledFor),
      lockedAt: data.lockedAt ? new Date(data.lockedAt) : null,
      completedAt: data.completedAt ? new Date(data.completedAt) : null,
      error: data.error || null,
      workerId: data.workerId || null,
    };
  }

  // this helps ensure we can parse the flat array returned by the Lua HGETALL command
  private parseHashResult(result: string[]): Job {
    const data: Record<string, string> = {};
    for (let i = 0; i < result.length; i += 2) {
      data[result[i]] = result[i + 1];
    }
    return this.deserializeJob(data);
  }
}
