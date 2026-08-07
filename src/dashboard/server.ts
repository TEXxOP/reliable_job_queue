import express from 'express';
import http from 'http';
import WebSocket from 'ws';
import path from 'path';
import Redis from 'ioredis';
import { RedisQueue } from '../queue/RedisQueue';
import { PostgresStorage } from '../storage/PostgresStorage';
import { config } from '../config';

// this helps ensure the dashboard can run as a standalone service
export async function startDashboard(): Promise<void> {
  const app = express();
  const server = http.createServer(app);
  const wss = new WebSocket.Server({ server });

  const redis = new Redis({
    host: config.redis.host,
    port: config.redis.port,
  });
  const redisQueue = new RedisQueue(redis);
  const storage = new PostgresStorage();

  await storage.runMigrations();

  // this helps ensure the dashboard UI files are served as static assets
  app.use(express.static(path.join(__dirname, 'public')));

  // this helps ensure we have REST endpoints for fetching metrics on demand
  app.get('/api/stats', async (_req, res) => {
    try {
      const [queueDepth, dbMetrics, completedCount, failedCount] = await Promise.all([
        redisQueue.getQueueDepth(),
        storage.getMetrics(),
        redisQueue.getCompletedCount(),
        redisQueue.getFailedCount(),
      ]);

      res.json({
        queue: queueDepth,
        database: dbMetrics,
        redis: { completed: completedCount, failed: failedCount },
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      res.status(500).json({ error: 'Failed to fetch stats' });
    }
  });

  app.get('/api/jobs', async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const jobs = await storage.getRecentJobs(limit);
      res.json(jobs);
    } catch (err) {
      res.status(500).json({ error: 'Failed to fetch jobs' });
    }
  });

  app.get('/api/dlq', async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const dlqJobs = await storage.getDeadLetterJobs(limit);
      res.json(dlqJobs);
    } catch (err) {
      res.status(500).json({ error: 'Failed to fetch DLQ jobs' });
    }
  });

  app.post('/api/dlq/recover/:id', async (req, res) => {
    try {
      const dlqId = req.params.id;
      const dlqRecord = await storage.markDeadLetterRecovered(dlqId);
      if (!dlqRecord) {
        return res.status(404).json({ error: 'DLQ job not found' });
      }

      const record = dlqRecord as Record<string, unknown>;
      const newJob = await redisQueue.enqueue({
        type: record.type as string,
        payload: (record.payload || {}) as Record<string, unknown>,
        priority: record.priority as 'normal',
        maxAttempts: record.max_attempts as number,
      });
      await storage.saveJob(newJob);

      res.json({ recovered: true, newJobId: newJob.id });
    } catch (err) {
      res.status(500).json({ error: 'Failed to recover DLQ job' });
    }
  });

  // this helps ensure the dashboard gets live updates via WebSocket every 2 seconds
  const broadcastMetrics = async () => {
    try {
      const [queueDepth, dbMetrics] = await Promise.all([
        redisQueue.getQueueDepth(),
        storage.getMetrics(),
      ]);

      const data = JSON.stringify({
        type: 'metrics',
        data: {
          queue: queueDepth,
          database: dbMetrics,
          timestamp: new Date().toISOString(),
        },
      });

      wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(data);
        }
      });
    } catch (err) {
      console.error('Broadcast error:', err);
    }
  };

  setInterval(broadcastMetrics, 2000);

  wss.on('connection', (ws) => {
    console.log('Dashboard client connected');
    // this helps ensure new clients get metrics immediately instead of waiting for the next tick
    broadcastMetrics();

    ws.on('close', () => {
      console.log('Dashboard client disconnected');
    });
  });

  server.listen(config.dashboard.port, () => {
    console.log(`Dashboard running at http://localhost:${config.dashboard.port}`);
  });
}

// this helps ensure the dashboard can run standalone or be imported
if (require.main === module) {
  startDashboard().catch(console.error);
}
