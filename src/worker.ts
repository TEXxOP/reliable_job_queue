import Redis from 'ioredis';
import { RedisQueue } from './queue/RedisQueue';
import { PostgresStorage } from './storage/PostgresStorage';
import { WorkerPool } from './worker/WorkerPool';
import { defaultHandlers } from './worker/handlers';
import { config } from './config';

// this helps ensure the worker process runs separately from the API server
async function main(): Promise<void> {
  console.log('Starting Job Queue Workers...');

  const redis = new Redis({
    host: config.redis.host,
    port: config.redis.port,
    retryStrategy: (times: number) => {
      const delay = Math.min(times * 200, 5000);
      console.log(`Redis connection attempt ${times}, retrying in ${delay}ms`);
      return delay;
    },
  });

  redis.on('connect', () => console.log('Connected to Redis'));
  redis.on('error', (err: Error) => console.error('Redis error:', err.message));

  const storage = new PostgresStorage();
  const redisQueue = new RedisQueue(redis);

  // this helps ensure database tables exist before workers start processing
  await storage.runMigrations();
  console.log('Database migrations applied');

  const pool = new WorkerPool(redis, redisQueue, storage);

  // this helps ensure all default job handlers are registered before starting
  for (const [type, handler] of defaultHandlers) {
    pool.registerHandler(type, handler);
  }

  await pool.start();
  console.log(`Worker pool started with concurrency: ${config.worker.concurrency}`);
}

main().catch((err) => {
  console.error('Failed to start workers:', err);
  process.exit(1);
});
