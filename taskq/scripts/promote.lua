-- promote.lua - atomically move every DUE member of a zset onto a list.
--
-- One script serves two reliability mechanisms (identical mechanics,
-- different source keys):
--   reaper:    in-flight zset -> ready queue  (lease expired = worker died)
--   scheduler: scheduled zset -> ready queue  (retry backoff has elapsed)
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
-- KEYS[2] = destination list (the ready queue)
-- ARGV[1] = max batch size per call. Redis is single-threaded: a script
--           runs to completion while everything else waits, so batches
--           stay small and bounded instead of "move everything".

-- Server-authoritative clock (see claim.lua for why).
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

-- Everything whose deadline/score is in the past is due.
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
if #due == 0 then
  return 0
end

-- Remove from the zset and push onto the queue. Order between the two is
-- irrelevant here precisely because the script is atomic: no observer
-- can ever see an id in both places or in neither.
redis.call('ZREM', KEYS[1], unpack(due))
for i = 1, #due do
  -- LPUSH puts recovered/retried jobs at the BACK of the FIFO line
  -- (RPOP serves from the right). Rescued work waits its turn like any
  -- newly enqueued job.
  redis.call('LPUSH', KEYS[2], due[i])
end

return #due
