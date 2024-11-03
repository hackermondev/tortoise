local index = KEYS[1]
local consumer = ARGV[1]
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local query = string.format('@metadata_assigned:{false} @metadata_completed:{false} @metadata_scheduled_retry_after:[-inf %d]', now)

-- Perform the search with the specified query and limit
local jobs = redis.call('FT.SEARCH', index, query, 'RETURN', '1', 'nonce', 'LIMIT', '0', limit)
local results = {}

-- Process each result in the chunk (skip the first element, which is the count)
for i = 2, #jobs, 2 do
    local job_id = jobs[i]

    -- Assign job to consumer
    redis.call('JSON.SET', job_id, '$.metadata.consumer_id', '"' .. consumer .. '"')
    redis.call('JSON.SET', job_id, '$.metadata.assigned', 'true')

    -- Add the job_id and associated field values to the results array
    table.insert(results, jobs[i])
    table.insert(results, jobs[i + 1]) -- This adds the field-value pairs
end

return results