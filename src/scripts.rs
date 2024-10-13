use std::sync::LazyLock;

use redis::Script;
pub static ATOMIC_MIGRATE_LIST: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        "
local chunk_size = 1000
local source = KEYS[1]
local destination = KEYS[2]

while true do
    local elements = redis.call('LRANGE', source, 0, chunk_size - 1)
    if next(elements) == nil then break end  -- Stop when there are no more elements
    redis.call('RPUSH', destination, unpack(elements))
    redis.call('LTRIM', source, chunk_size, -1)  -- Remove the elements from the source
end

return redis.call('LLEN', destination)  -- Return the number of elements in the destination list
",
    )
});
