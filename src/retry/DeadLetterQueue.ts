import { Job, CreateJobInput } from '../queue/types';
import { RedisQueue } from '../queue/RedisQueue';
import { PostgresStorage } from '../storage/PostgresStorage';

export class DeadLetterQueue {
  private redisQueue: RedisQueue;
  private storage: PostgresStorage;

  constructor(redisQueue: RedisQueue, storage: PostgresStorage) {
    this.redisQueue = redisQueue;
    this.storage = storage;
  }

  // this helps ensure permanently failed jobs are moved out of the active queue
  // and stored in PostgreSQL where they can be inspected later
  async moveToDeadLetter(job: Job, reason: string): Promise<void> {
    await this.redisQueue.moveToDead(job.id, reason);
    await this.storage.saveToDeadLetter(job, reason);
    await this.storage.updateJobStatus(job.id, 'dead', { error: reason });

    console.log(`Job ${job.id} (${job.type}) moved to DLQ: ${reason}`);
  }

  // this helps ensure we can re-enqueue a failed job for another try after fixing the issue
  async recoverJob(dlqId: string): Promise<Job | null> {
    const dlqRecord = await this.storage.markDeadLetterRecovered(dlqId);
    if (!dlqRecord) return null;

    const record = dlqRecord as Record<string, unknown>;

    // this helps ensure the recovered job starts fresh with reset attempts
    const input: CreateJobInput = {
      type: record.type as string,
      payload: (record.payload || {}) as Record<string, unknown>,
      priority: record.priority as CreateJobInput['priority'],
      maxAttempts: record.max_attempts as number,
    };

    const newJob = await this.redisQueue.enqueue(input);
    await this.storage.saveJob(newJob);

    console.log(`DLQ job ${dlqId} recovered as new job ${newJob.id}`);
    return newJob;
  }

  // this helps ensure we can recover all failed jobs of a specific type at once
  async recoverByType(jobType: string): Promise<number> {
    const deadJobs = await this.storage.getDeadLetterJobs(1000);
    let recoveredCount = 0;

    for (const record of deadJobs) {
      const dlqJob = record as Record<string, unknown>;
      if (dlqJob.type === jobType) {
        await this.recoverJob(dlqJob.id as string);
        recoveredCount++;
      }
    }

    console.log(`Recovered ${recoveredCount} DLQ jobs of type "${jobType}"`);
    return recoveredCount;
  }

  // this helps ensure the dashboard can display all dead letter jobs
  async listDeadLetterJobs(limit: number = 50): Promise<unknown[]> {
    return this.storage.getDeadLetterJobs(limit);
  }
}
