import Redis from 'ioredis';
import { RedisQueue } from './queue/RedisQueue';
import { PostgresStorage } from './storage/PostgresStorage';
import { CreateJobInput, JobPriority } from './queue/types';
import { config } from './config';

// this helps ensure we have realistic test data to demo the system
const SEED_JOBS: CreateJobInput[] = [
  // high-priority emails
  { type: 'email', payload: { to: 'ceo@company.com', subject: 'Urgent: Server Alert', body: 'CPU at 95%' }, priority: 'critical' },
  { type: 'email', payload: { to: 'team@company.com', subject: 'Sprint Review', body: 'Join at 3pm' }, priority: 'high' },
  { type: 'email', payload: { to: 'user1@test.com', subject: 'Welcome!', body: 'Thanks for signing up' }, priority: 'normal' },
  { type: 'email', payload: { to: 'user2@test.com', subject: 'Password Reset', body: 'Click here to reset' }, priority: 'high' },
  { type: 'email', payload: { to: 'newsletter@test.com', subject: 'Weekly Digest', body: 'Top stories this week' }, priority: 'low' },

  // payment jobs
  { type: 'payment', payload: { orderId: 'ORD-1001', amount: 299.99, currency: 'USD' }, priority: 'critical' },
  { type: 'payment', payload: { orderId: 'ORD-1002', amount: 49.99, currency: 'USD' }, priority: 'high' },
  { type: 'payment', payload: { orderId: 'ORD-1003', amount: 149.50, currency: 'EUR' }, priority: 'normal' },
  { type: 'payment', payload: { orderId: 'ORD-1004', amount: 599.00, currency: 'GBP' }, priority: 'high' },
  { type: 'payment', payload: { orderId: 'ORD-1005', amount: 19.99, currency: 'USD' }, priority: 'low' },

  // report generation
  { type: 'report', payload: { reportType: 'monthly-revenue', userId: 'admin-1' }, priority: 'normal' },
  { type: 'report', payload: { reportType: 'user-analytics', userId: 'admin-2' }, priority: 'low' },
  { type: 'report', payload: { reportType: 'inventory-audit', userId: 'admin-1' }, priority: 'normal' },
  { type: 'report', payload: { reportType: 'compliance-check', userId: 'admin-3' }, priority: 'high' },
  { type: 'report', payload: { reportType: 'daily-summary', userId: 'admin-2' }, priority: 'low' },
];

async function seed(): Promise<void> {
  const count = parseInt(process.argv[2] || '15', 10);
  console.log(`Seeding ${count} jobs...`);

  const redis = new Redis({ host: config.redis.host, port: config.redis.port });
  const redisQueue = new RedisQueue(redis);
  const storage = new PostgresStorage();

  await storage.runMigrations();

  // this helps ensure we cycle through the templates when count > 15
  for (let i = 0; i < count; i++) {
    const template = SEED_JOBS[i % SEED_JOBS.length];
    const job = await redisQueue.enqueue(template);
    await storage.saveJob(job);
    console.log(`  [${job.priority.toUpperCase()}] ${job.type} → ${job.id.substring(0, 8)}...`);
  }

  console.log(`\nDone! ${count} jobs seeded.`);
  console.log('Watch them process: http://localhost:3001');

  await storage.close();
  redis.disconnect();
}

seed().catch((err) => {
  console.error('Seed failed:', err);
  process.exit(1);
});
