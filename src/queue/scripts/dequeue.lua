-- this helps ensure dequeue is atomic: no two workers can grab the same job
-- KEYS[1] = pending sorted set (e.g. "queue:pending")
-- KEYS[2] = processing sorted set (e.g. "queue:processing")
-- KEYS[3] = job data hash prefix (e.g. "queue:job:")
-- ARGV[1] = current timestamp (seconds)
-- ARGV[2] = visibility timeout (seconds)
-- ARGV[3] = worker id

-- find the highest-priority job that is ready to be processed
-- jobs are scored by: priority_score * 1e10 + scheduled_timestamp
-- we only pick jobs whose score allows them to run now
local candidates = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '+inf', 'LIMIT', 0, 1)

if #candidates == 0 then
  return nil
end

local jobId = candidates[1]

-- move job from pending to processing atomically
redis.call('ZREM', KEYS[1], jobId)

-- set the visibility timeout: current time + timeout duration
local visibilityDeadline = tonumber(ARGV[1]) + tonumber(ARGV[2])
redis.call('ZADD', KEYS[2], visibilityDeadline, jobId)

-- update the job metadata in its hash
local jobKey = KEYS[3] .. jobId
redis.call('HSET', jobKey, 'status', 'processing')
redis.call('HSET', jobKey, 'lockedAt', ARGV[1])
redis.call('HSET', jobKey, 'workerId', ARGV[3])

-- return the full job data so the worker doesn't need a second round-trip
return redis.call('HGETALL', jobKey)
