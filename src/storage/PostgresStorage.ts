import { Pool } from 'pg';
import fs from 'fs';
import path from 'path';
import { Job } from '../queue/types';
import { config } from '../config';

export class PostgresStorage {
  private pool: Pool;

  constructor() {
    this.pool = new Pool({
      host: config.postgres.host,
      port: config.postgres.port,
      database: config.postgres.database,
      user: config.postgres.user,
      password: config.postgres.password,
    });
  }

  // this helps ensure the database schema is set up before any jobs are processed
  async runMigrations(): Promise<void> {
    const migrationsDir = path.join(__dirname, 'migrations');
    const files = fs.readdirSync(migrationsDir).sort();

    for (const file of files) {
      if (file.endsWith('.sql')) {
        const sql = fs.readFileSync(path.join(migrationsDir, file), 'utf-8');
        await this.pool.query(sql);
        console.log(`Migration applied: ${file}`);
      }
    }
  }

  // this helps ensure every job is persisted to PostgreSQL for durability
  async saveJob(job: Job): Promise<void> {
    await this.pool.query(
      `INSERT INTO jobs (id, type, payload, priority, status, attempts, max_attempts, 
        created_at, updated_at, scheduled_for, locked_at, completed_at, error, worker_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
       ON CONFLICT (id) DO UPDATE SET
         status = EXCLUDED.status,
         attempts = EXCLUDED.attempts,
         updated_at = EXCLUDED.updated_at,
         locked_at = EXCLUDED.locked_at,
         completed_at = EXCLUDED.completed_at,
         error = EXCLUDED.error,
         worker_id = EXCLUDED.worker_id`,
      [
        job.id, job.type, JSON.stringify(job.payload), job.priority, job.status,
        job.attempts, job.maxAttempts, job.createdAt, job.updatedAt, job.scheduledFor,
        job.lockedAt, job.completedAt, job.error, job.workerId,
      ]
    );
  }

  // this helps ensure we can update just the status without rewriting the whole job
  async updateJobStatus(
    jobId: string,
    status: string,
    updates: Partial<{ attempts: number; error: string; workerId: string; lockedAt: Date; completedAt: Date }>
  ): Promise<void> {
    const setClauses = ['status = $2', 'updated_at = NOW()'];
    const params: unknown[] = [jobId, status];
    let paramIndex = 3;

    if (updates.attempts !== undefined) {
      setClauses.push(`attempts = $${paramIndex++}`);
      params.push(updates.attempts);
    }
    if (updates.error !== undefined) {
      setClauses.push(`error = $${paramIndex++}`);
      params.push(updates.error);
    }
    if (updates.workerId !== undefined) {
      setClauses.push(`worker_id = $${paramIndex++}`);
      params.push(updates.workerId);
    }
    if (updates.lockedAt !== undefined) {
      setClauses.push(`locked_at = $${paramIndex++}`);
      params.push(updates.lockedAt);
    }
    if (updates.completedAt !== undefined) {
      setClauses.push(`completed_at = $${paramIndex++}`);
      params.push(updates.completedAt);
    }

    await this.pool.query(
      `UPDATE jobs SET ${setClauses.join(', ')} WHERE id = $1`,
      params
    );
  }

  // this helps ensure we can look up any job for the API or dashboard
  async getJobById(jobId: string): Promise<Job | null> {
    const result = await this.pool.query('SELECT * FROM jobs WHERE id = $1', [jobId]);
    if (result.rows.length === 0) return null;
    return this.rowToJob(result.rows[0]);
  }

  // this helps ensure the dashboard can show recent job activity
  async getRecentJobs(limit: number = 50): Promise<Job[]> {
    const result = await this.pool.query(
      'SELECT * FROM jobs ORDER BY created_at DESC LIMIT $1',
      [limit]
    );
    return result.rows.map(this.rowToJob);
  }

  // this helps ensure the dashboard has accurate metrics to display
  async getMetrics(): Promise<{
    pending: number;
    processing: number;
    completed: number;
    failed: number;
    dead: number;
    recentThroughput: number;
  }> {
    const statusCounts = await this.pool.query(`
      SELECT status, COUNT(*)::int as count 
      FROM jobs 
      GROUP BY status
    `);

    const counts: Record<string, number> = {};
    for (const row of statusCounts.rows) {
      counts[row.status] = row.count;
    }

    // this helps ensure throughput is calculated over the last minute
    const throughputResult = await this.pool.query(`
      SELECT COUNT(*)::int as count 
      FROM jobs 
      WHERE status = 'completed' 
        AND completed_at > NOW() - INTERVAL '1 minute'
    `);

    const dlqCount = await this.pool.query(`
      SELECT COUNT(*)::int as count 
      FROM dead_letter_jobs 
      WHERE recovered_at IS NULL
    `);

    return {
      pending: counts['pending'] || 0,
      processing: counts['processing'] || 0,
      completed: counts['completed'] || 0,
      failed: counts['failed'] || 0,
      dead: dlqCount.rows[0].count,
      recentThroughput: throughputResult.rows[0].count,
    };
  }

  // this helps ensure we can save failed jobs to the dead letter table
  async saveToDeadLetter(job: Job, reason: string): Promise<void> {
    await this.pool.query(
      `INSERT INTO dead_letter_jobs (original_job_id, type, payload, priority, attempts, max_attempts, error, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [job.id, job.type, JSON.stringify(job.payload), job.priority, job.attempts, job.maxAttempts, reason, job.createdAt]
    );
  }

  // this helps ensure we can list dead letter jobs for the dashboard
  async getDeadLetterJobs(limit: number = 50): Promise<unknown[]> {
    const result = await this.pool.query(
      `SELECT * FROM dead_letter_jobs WHERE recovered_at IS NULL ORDER BY failed_at DESC LIMIT $1`,
      [limit]
    );
    return result.rows;
  }

  // this helps ensure we can recover a specific dead letter job by marking it recovered
  async markDeadLetterRecovered(dlqId: string): Promise<unknown> {
    const result = await this.pool.query(
      `UPDATE dead_letter_jobs SET recovered_at = NOW() WHERE id = $1 RETURNING *`,
      [dlqId]
    );
    return result.rows[0] || null;
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  // this helps ensure database rows map cleanly to our Job type
  private rowToJob(row: Record<string, unknown>): Job {
    return {
      id: row.id as string,
      type: row.type as string,
      payload: (row.payload || {}) as Record<string, unknown>,
      priority: row.priority as Job['priority'],
      status: row.status as Job['status'],
      attempts: row.attempts as number,
      maxAttempts: row.max_attempts as number,
      createdAt: new Date(row.created_at as string),
      updatedAt: new Date(row.updated_at as string),
      scheduledFor: new Date(row.scheduled_for as string),
      lockedAt: row.locked_at ? new Date(row.locked_at as string) : null,
      completedAt: row.completed_at ? new Date(row.completed_at as string) : null,
      error: (row.error as string) || null,
      workerId: (row.worker_id as string) || null,
    };
  }
}
