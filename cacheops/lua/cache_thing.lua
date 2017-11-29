local prefix = KEYS[1]
local key = KEYS[2]
local data = ARGV[1]
local dnfs = cjson.decode(ARGV[2])
local timeout = tonumber(ARGV[3])

local timeout_increment = timeout * 2 + 10

-- Write data to cache
redis.call('setex', key, timeout, data)

-- Update schemes and invalidators
for db_table, disj in pairs(dnfs) do

    local schemes_key = prefix .. 'schemes:' .. db_table
    local conj_key_prefix = prefix .. 'conj:' .. db_table .. ':'

    for _, conj in ipairs(disj) do

        local parts = {}
        local parts2 = {}

        for field, val in pairs(conj) do
            table.insert(parts, field)
            table.insert(parts2, field .. '=' .. tostring(val))
        end

        local conj_schema = table.concat(parts, ',')
        local conj_key = conj_key_prefix .. table.concat(parts2, '&')

        -- Ensure scheme is known
        redis.call('sadd', schemes_key, conj_schema)

        -- Add new cache_key to list of dependencies
        redis.call('sadd', conj_key, key)
        -- NOTE: an invalidator should live longer than any key it references.
        --       So we update its ttl on every key if needed.
        -- NOTE: if CACHEOPS_LRU is True when invalidators should be left persistent,
        --       so we strip next section from this script.
        -- TOSTRIP
        if redis.call('ttl', conj_key) < timeout then
            -- We set conj_key life with a margin over key lifes to call expire rarer
            -- And add few extra seconds to be extra safe
            redis.call('expire', conj_key, timeout_increment)
        end
        -- /TOSTRIP
    end
end
