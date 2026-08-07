// this helps ensure all job-related types are consistent across the codebase

export type JobStatus = 'pending' | 'processing' | 'completed' | 'failed' | 'dead';

export type JobPriority = 'critical' | 'high' | 'normal' | 'low';

// this helps ensure priority values map to scores so Redis sorted sets order them correctly
// lower score = higher priority = gets picked up first
export const PRIORITY_SCORES: Record<JobPriority, number> = {
  critical: 0,
  high: 10,
  normal: 20,
  low: 30,
};

export interface Job {
  id: string;
  type: string;
  payload: Record<string, unknown>;
  priority: JobPriority;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  createdAt: Date;
  updatedAt: Date;
  scheduledFor: Date;
  lockedAt: Date | null;
  completedAt: Date | null;
  error: string | null;
  workerId: string | null;
}

// this helps ensure we have a clean interface for creating new jobs without needing all fields
export interface CreateJobInput {
  type: string;
  payload: Record<string, unknown>;
  priority?: JobPriority;
  maxAttempts?: number;
  scheduledFor?: Date;
}

export interface QueueConfig {
  visibilityTimeoutSec: number;
  pollIntervalMs: number;
}

export interface RetryConfig {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

// this helps ensure job handlers have a predictable signature
export type JobHandler = (job: Job) => Promise<void>;

export interface JobResult {
  success: boolean;
  error?: string;
  duration: number;
}

export interface QueueStats {
  pending: number;
  processing: number;
  completed: number;
  failed: number;
  dead: number;
  throughputPerMinute: number;
}
