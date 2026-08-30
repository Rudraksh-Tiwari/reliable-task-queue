-- replay.lua - atomically move a job OUT of the dead-letter queue and
-- back into its priority lane with a fresh retry budget.
--
-- Why atomic: the dashboard can be open in several tabs, and two people
-- can click "replay" on the same dead job at the same moment. Done as
-- separate commands (LREM, then HSET, then LPUSH) both clicks would push
-- the SAME id into the lane and the job would run twice. Here LREM is the
-- gate: it reports how many entries it actually removed, so only the
-- caller that genuinely took the job off the dead list proceeds. The
-- loser removes nothing and returns 0.
--
-- KEYS[1] = dead-letter list
-- ARGV[1] = job id to replay
-- ARGV[2] = job-hash key prefix ("taskq:job:")
-- ARGV[3] = lane key prefix ("taskq:queue:default:")

-- LREM count=0 means "remove all matching entries"; it returns how many
-- were removed. Zero means the job was not on the dead list (already
-- replayed, discarded, or never dead) - so there is nothing to do.
local removed = redis.call('LREM', KEYS[1], 0, ARGV[1])
if removed == 0 then
  return 0
end

local job_key = ARGV[2] .. ARGV[1]

-- The payload must still exist to be runnable. If an operator deleted it,
-- we have already taken the id off the dead list above; returning -1 lets
-- the API report "payload gone" rather than queueing an unrunnable id.
if redis.call('EXISTS', job_key) == 0 then
  return -1
end

-- Reset the execution state so the job gets a FULL retry budget again.
-- Replay means "the underlying problem is fixed, try this properly" - not
-- "burn the one attempt it had left". claim.lua will HINCRBY attempts up
-- from 0 on the next claim.
redis.call('HSET', job_key, 'attempts', 0, 'status', 'queued', 'error', '')

-- Send it back to its OWN priority lane, exactly as crash recovery does.
local prio = redis.call('HGET', job_key, 'priority')
if prio ~= 'high' and prio ~= 'low' then
  prio = 'normal'
end
redis.call('LPUSH', ARGV[3] .. prio, ARGV[1])

return 1
