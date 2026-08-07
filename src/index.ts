import Redis from 'ioredis';
import { RedisQueue } from './queue/RedisQueue';
import { PostgresStorage } from './storage/PostgresStorage';
import { createApiServer } from './api/server';
import { config } from './config';

// this helps ensure the API server is the main entry point for producers
async function main(): Promise<void> {
  console.log('Starting Job Queue API Server...');

  const redis = new Redis({
    host: config.redis.host,
    port: config.redis.port,
    retryStrategy: (times: number) => {
      // this helps ensure we keep trying to connect to Redis on startup
      const delay = Math.min(times * 200, 5000);
      console.log(`Redis connection attempt ${times}, retrying in ${delay}ms`);
      return delay;
    },
  });

  redis.on('connect', () => console.log('Connected to Redis'));
  redis.on('error', (err: Error) => console.error('Redis error:', err.message));

  const storage = new PostgresStorage();
  const redisQueue = new RedisQueue(redis);

  // this helps ensure database tables exist before accepting requests
  await storage.runMigrations();
  console.log('Database migrations applied');

  const app = createApiServer(redis, redisQueue, storage);

  app.listen(config.api.port, () => {
    console.log(`API server running at http://localhost:${config.api.port}`);
    console.log('Endpoints:');
    console.log(`  POST   /api/jobs     - Enqueue a new job`);
    console.log(`  GET    /api/jobs/:id  - Get job status`);
    console.log(`  DELETE /api/jobs/:id  - Cancel a pending job`);
    console.log(`  GET    /api/stats    - Queue statistics`);
    console.log(`  GET    /api/health   - Health check`);
  });
}

main().catch((err) => {
  console.error('Failed to start API server:', err);
  process.exit(1);
});
