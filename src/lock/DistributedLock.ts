import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

// this helps ensure the release script only unlocks if the caller is the actual owner
// prevents one worker from accidentally releasing another worker's lock
const RELEASE_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`;

// this helps ensure lock extension only works if the caller still owns the lock
const EXTEND_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
  else
    return 0
  end
`;

export class DistributedLock {
  private redis: Redis;
  private ownerId: string;

  constructor(redis: Redis) {
    this.redis = redis;
    // this helps ensure each worker instance has a unique identity for lock ownership
    this.ownerId = uuidv4();
  }

  // this helps ensure only one worker processes a given resource at a time
  // uses SET NX PX for atomic "set if not exists" with automatic expiry
  async acquire(resource: string, ttlMs: number): Promise<boolean> {
    const lockKey = `lock:${resource}`;
    const result = await this.redis.set(lockKey, this.ownerId, 'PX', ttlMs, 'NX');
    return result === 'OK';
  }

  // this helps ensure locks are released safely - only the owner can release their own lock
  async release(resource: string): Promise<boolean> {
    const lockKey = `lock:${resource}`;
    const result = await this.redis.eval(RELEASE_SCRIPT, 1, lockKey, this.ownerId) as number;
    return result === 1;
  }

  // this helps ensure long-running jobs can extend their lock before it expires
  async extend(resource: string, ttlMs: number): Promise<boolean> {
    const lockKey = `lock:${resource}`;
    const result = await this.redis.eval(EXTEND_SCRIPT, 1, lockKey, this.ownerId, ttlMs) as number;
    return result === 1;
  }

  getOwnerId(): string {
    return this.ownerId;
  }
}
