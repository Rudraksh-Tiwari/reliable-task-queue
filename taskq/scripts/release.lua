-- release.lua - free an idempotency slot ONLY if this job still owns it.
--
-- This is the classic "safe lock release" pattern (same as Redlock's
-- release step). The naive version - GET, compare in Python, then DEL -
-- is two commands with a race between them: our idempotency key could
-- expire (TTL) and be re-taken by a NEWER job in that gap, and our DEL
-- would then destroy the new job's dedup slot, silently re-opening the
-- door to duplicates. Comparing and deleting inside one atomic script
-- makes a stale releaser harmless: if the value is no longer our job id,
-- we touch nothing.
--
-- KEYS[1] = idempotency key ("taskq:idem:<user key>")
-- ARGV[1] = the job id that believes it owns the slot

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
