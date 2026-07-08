-- promote.lua - atomically move every DUE member of a zset back into the
-- ready lanes.
--
-- One script serves two reliability mechanisms (identical mechanics,
-- different source keys):
--   reaper:    in-flight zset -> ready lanes  (lease expired = worker died)
--   scheduler: scheduled zset -> ready lanes  (retry backoff elapsed, or a
--                                              delayed job's run-at arrived)
--
-- Why atomic: done as separate client commands (ZRANGEBYSCORE, then
-- LPUSH + ZREM), two concurrent reapers could both read the same due id
-- and requeue it twice, or race against an ack that is removing it.
-- Inside one script nothing interleaves, so ANY number of reapers can
-- run concurrently with no locks and no leader election - which is what
-- lets every worker carry its own reaper thread and the fleet heal
-- itself as long as one worker is alive.
--
-- KEYS[1] = source zset (in-flight OR scheduled), scored by unix deadline
-- ARGV[1] = max batch size per call. Redis is single-threaded: a script
--           runs to completion while everything else waits, so batches
--           stay small and bounded instead of "move everything".
-- ARGV[2] = job-hash key prefix ("taskq:job:") - to read each job's priority
-- ARGV[3] = lane key prefix ("taskq:queue:default:") - destination lanes

-- Server-authoritative clock (see claim.lua for why).
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

-- Everything whose deadline/score is in the past is due.
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
if #due == 0 then
  return 0
end

-- Remove from the zset first; the loop below re-homes every id. Order
-- between the two is irrelevant precisely because the script is atomic:
-- no observer can ever see an id in both places or in neither.
redis.call('ZREM', KEYS[1], unpack(due))

for i = 1, #due do
  local id = due[i]
  -- Each job returns to ITS OWN priority lane, read from its hash.
  -- Recovery must not silently demote work: a high-priority job whose
  -- worker died re-enters the HIGH lane, not the back of normal.
  local prio = redis.call('HGET', ARGV[2] .. id, 'priority')
  -- Unknown/missing priority (legacy or orphaned entries) lands in
  -- 'normal'; if the payload is gone entirely, claim.lua will drop it.
  if prio ~= 'high' and prio ~= 'low' then
    prio = 'normal'
  end
  -- LPUSH puts recovered/retried jobs at the BACK of their FIFO lane
  -- (RPOP serves from the right). Rescued work waits its turn like any
  -- newly enqueued job of the same priority.
  redis.call('LPUSH', ARGV[3] .. prio, id)
end

return #due
