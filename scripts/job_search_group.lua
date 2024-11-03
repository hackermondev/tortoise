local index = KEYS[1]
local consumer = ARGV[1]
local job_prefix = ARGV[2]
local limit = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local query = string.format('@metadata_assigned:{false} @metadata_completed:{false} @metadata_scheduled_retry_after:[-inf %d]', now)

-- Perform the search with the specified query, aggregation, and limit
local jobs = redis.call('FT.AGGREGATE', index, query, 'LOAD', '1', 'nonce', 'GROUPBY', '1', '@metadata_group_tag', 'REDUCE', 'TOLIST', '1', '@nonce', 'LIMIT', '0', limit)

-- Check the number of elements returned
local num_elements = tonumber(jobs[1])
if num_elements == 0 then 
    return {}  -- Return an empty array if there are no results
end

-- Initialize the results array
local results = {}
local job_group = jobs[2]
local job_list = job_group[4]  -- Adjusted to use a distinct variable name

-- Iterate over each job and update the JSON data
for i = 1, #job_list, 1 do
    local job_id = job_list[i]
    local job_key = job_prefix .. job_id

    -- Assign job to consumer
    redis.call('JSON.SET', job_key, '$.metadata.consumer_id', '"' .. consumer .. '"')
    redis.call('JSON.SET', job_key, '$.metadata.assigned', 'true')

    -- Add the job_key to the results array
    table.insert(results, job_id)
end

return results
