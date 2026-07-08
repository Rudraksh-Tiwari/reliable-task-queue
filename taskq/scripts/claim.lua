-- claim.lua - atomically claim the next job across the priority lanes.
--
-- WHY THIS IS A SCRIPT (the core interview answer):
-- A naive worker runs two commands: RPOP (take the job) and then ZADD
-- (record "I am working on it until time T"). Between those two commands
-- the worker can crash. At that instant the job is in NOBODY's hands:
-- it has left the queue but never entered the in-flight set, so the
-- reaper will never find it. The job is silently lost - the exact
-- failure a task queue exists to prevent.
--
-- Redis executes a Lua script as one atomic unit: no other command from
-- any client interleaves with it. So "pop + record lease + count the
-- attempt" become a single indivisible CLAIM. Either the job stays
-- queued, or it is in-flight with a lease - there is no in-between
-- state, no matter when the worker dies. This also makes MANY workers
-- safe: two workers can never claim the same job, because their claims
-- are serialized by Redis itself.
--
-- KEYS[1] = high-priority lane     (list, e.g. "taskq:queue:default:high")
-- KEYS[2] = normal-priority lane   (list, e.g. "taskq:queue:default:normal")
-- KEYS[3] = low-priority lane      (list, e.g. "taskq:queue:default:low")
-- KEYS[4] = in-flight zset         (e.g. "taskq:inflight:default")
-- ARGV[1] = lease duration in seconds
-- ARGV[2] = job-hash key prefix    (e.g. "taskq:job:")
--
-- Note on ARGV[2]: the job's hash key is built inside the script because
-- we cannot know the job id until after RPOP. Building key names in Lua
-- is fine on a single Redis instance but is not Redis-Cluster-safe (the
-- cluster router must know all keys up front). Deliberate tradeoff,
-- documented here and in the README.

-- Try the lanes strictly in priority order: high, then normal, then low.
-- STRICT priority is a deliberate tradeoff: it is simple and predictable,
-- but a constant flood of high-priority work can starve the low lane.
-- Real systems that need fairness use weighted dequeue (e.g. pick high
-- with probability 0.7); strict is the right default for a queue where
-- "high" means "genuinely urgent and rare".
local id
for i = 1, 3 do
  -- Producers LPUSH onto the left end, so the right end is the front of
  -- each FIFO lane. In Lua, RPOP returns boolean false when empty.
  id = redis.call('RPOP', KEYS[i])
  if id then
    break
  end
end

-- All three lanes empty: return nil so the worker knows to idle.
if not id then
  return nil
end

-- Build the key of this job's hash, e.g. "taskq:job:8d62e5fc...".
local job_key = ARGV[2] .. id

-- Fetch the immutable job description (the JSON blob written at enqueue
-- time: task name, args, kwargs, max_retries, priority, ...).
local data = redis.call('HGET', job_key, 'data')

-- Defensive check: an id whose payload hash is missing. Our enqueue
-- writes hash + id inside one MULTI so this "cannot happen", but if it
-- ever does (manual key deletion, bugs), dropping the orphan id beats
-- crashing every worker that pops it. Returning nil means "try again".
if not data then
  return nil
end

-- Ask REDIS for the current time instead of trusting the worker's clock.
-- Different worker machines have slightly different clocks (skew); using
-- the server's single clock means leases expire consistently no matter
-- who claimed the job. TIME returns {seconds, microseconds}.
-- (Safe alongside writes on Redis >= 5: scripts replicate by effects.)
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

-- Count this execution attempt NOW, atomically with the claim itself,
-- not later in worker code. If a "poison pill" job crashes the worker
-- process before any Python code could increment a counter, the counter
-- would never rise and the job would crash workers in an infinite loop
-- forever. Incrementing at claim time guarantees every start is counted,
-- which makes max_retries enforceable even for jobs that kill their
-- worker. HINCRBY returns the new value.
local attempts = redis.call('HINCRBY', job_key, 'attempts', 1)

-- Advisory label for humans and dashboards. The AUTHORITATIVE state is
-- which structure the id currently lives in (lane / in-flight /
-- scheduled / dead): a label can go stale, set membership cannot.
redis.call('HSET', job_key, 'status', 'running')

-- Record the lease: "this job is being worked on until <deadline>".
-- If the worker finishes, ack removes this entry. If the worker dies,
-- the entry outlives it, the deadline passes, and the reaper moves the
-- id back to its priority lane. This one zset entry is what makes a
-- crashed worker's job recoverable instead of lost.
local deadline = now + tonumber(ARGV[1])
redis.call('ZADD', KEYS[4], deadline, id)

-- Give the worker everything it needs in a single round-trip:
-- the id, the immutable payload, and the authoritative attempt number.
return { id, data, attempts }
