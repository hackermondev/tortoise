-- Atomically drop all jobs assigned to a consumer

local chunk_size = 100
local index = KEYS[1]
local consumer = ARGV[1]

local query = '@metadata_consumer_id:' .. consumer:gsub("%\\\\", "%\\")
local count = 0

while true do
    -- Search for jobs assigned to the specified consumer
    local elements = redis.call('FT.SEARCH', index, query, 'RETURN', '1', 'nonce', 'LIMIT', '0', chunk_size)
    local num_elements = tonumber(elements[1])

    -- Stop if no more elements are found
    if num_elements == 0 then break end

    -- Process each result in the chunk (skip the first element, which is the count)
    for i = 2, #elements, 2 do
        local job_id = elements[i]

        -- Unassign job
        redis.call('JSON.SET', job_id, '$.metadata.assigned', 'false')
        redis.call('JSON.SET', job_id, '$.metadata.consumer_id', 'null')
        
        -- Increment the count of modified jobs
        count = count + 1
    end
end

return count