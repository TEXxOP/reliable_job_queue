import express from 'express';
import Redis from 'ioredis';
import { RedisQueue } from '../queue/RedisQueue';
import { PostgresStorage } from '../storage/PostgresStorage';
import { config } from '../config';

// this helps ensure the API server is created as a reusable function
export function createApiServer(redis: Redis, redisQueue: RedisQueue, storage: PostgresStorage): express.Express {
  const app = express();
  app.use(express.json());

  // this helps ensure clients can check if the service is healthy
  app.get('/api/health', async (_req, res) => {
    try {
      await redis.ping();
      res.json({ status: 'healthy', timestamp: new Date().toISOString() });
    } catch (err) {
      res.status(503).json({ status: 'unhealthy', error: 'Redis connection failed' });
    }
  });

  // this helps ensure producers can submit new jobs to the queue
  app.post('/api/jobs', async (req, res) => {
    try {
      const { type, payload, priority, maxAttempts, scheduledFor } = req.body;

      if (!type) {
        return res.status(400).json({ error: 'Job type is required' });
      }

      const job = await redisQueue.enqueue({
        type,
        payload: payload || {},
        priority: priority || 'normal',
        maxAttempts,
        scheduledFor: scheduledFor ? new Date(scheduledFor) : undefined,
      });

      // this helps ensure the job is also persisted in PostgreSQL for durability
      await storage.saveJob(job);

      res.status(201).json(job);
    } catch (err) {
      console.error('Failed to enqueue job:', err);
      res.status(500).json({ error: 'Failed to enqueue job' });
    }
  });

  // this helps ensure clients can check the status of a specific job
  app.get('/api/jobs/:id', async (req, res) => {
    try {
      const job = await storage.getJobById(req.params.id);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      res.json(job);
    } catch (err) {
      res.status(500).json({ error: 'Failed to fetch job' });
    }
  });

  // this helps ensure we have a way to view queue metrics from the API
  app.get('/api/stats', async (_req, res) => {
    try {
      const [queueDepth, dbMetrics] = await Promise.all([
        redisQueue.getQueueDepth(),
        storage.getMetrics(),
      ]);

      res.json({
        queue: queueDepth,
        database: dbMetrics,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      res.status(500).json({ error: 'Failed to fetch stats' });
    }
  });

  // this helps ensure pending jobs can be cancelled before they are picked up
  app.delete('/api/jobs/:id', async (req, res) => {
    try {
      const job = await storage.getJobById(req.params.id);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      if (job.status !== 'pending') {
        return res.status(409).json({ error: `Cannot cancel job with status: ${job.status}` });
      }

      await storage.updateJobStatus(req.params.id, 'failed', { error: 'Cancelled by user' });
      res.json({ cancelled: true, jobId: req.params.id });
    } catch (err) {
      res.status(500).json({ error: 'Failed to cancel job' });
    }
  });

  return app;
}
