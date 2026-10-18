-- discard.lua - permanently drop a job from the dead-letter queue, but ONLY
-- if it is still on the dead-letter list.
--
-- Why atomic and conditional: discard used to be a pipeline of LREM + DEL,
-- and the DEL ran whether or not the LREM found anything. Two operators
-- clicking on the same dead job show why that loses work: the first clicks
-- "replay", replay.lua takes the id off the dead list and pushes it back to
-- its lane; the second clicks "discard" a moment later, its LREM removes
-- nothing (the id is no longer dead), but its DEL still deletes the payload
-- of a job that is now queued. claim.lua later pops that id, finds no
-- payload, and drops it. The job is lost, and the API even answered 404.
--
-- Here LREM is the gate, exactly as in replay.lua: only the caller that
-- actually took the id off the dead list may delete the payload. Discarding
-- an id that is queued, in flight, scheduled, already replayed, or unknown
-- touches nothing.
--
-- KEYS[1] = dead-letter list
-- KEYS[2] = the job's hash ("taskq:job:<id>")
-- ARGV[1] = job id to discard

local removed = redis.call('LREM', KEYS[1], 0, ARGV[1])
if removed == 0 then
  return 0
end

redis.call('DEL', KEYS[2])
return 1
