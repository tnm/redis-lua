module('Redis', package.seeall)

local socket = require('socket')       -- requires LuaSocket as a dependency
local uri    = require('socket.url')

local redis_commands = {}
local network, request, response = {}, {}, {}, {}

local defaults = { host = '127.0.0.1', port = 6379 }
local protocol = { newline = '\r\n', ok = 'OK', err = 'ERR', null = 'nil' }

local function toboolean(value) return value == 1 end

local function load_methods(proto, methods)
    local redis = setmetatable ({}, getmetatable(proto))
    for i, v in pairs(proto) do redis[i] = v end

    for i, v in pairs(methods) do redis[i] = v end
    return redis
end

-- ############################################################################

function network.write(client, buffer)
    local _, err = client.socket:send(buffer)
    if err then error(err) end
end

function network.read(client, len)
    if len == nil then len = '*l' end
    local line, err = client.socket:receive(len)
    if not err then return line else error('Connection error: ' .. err) end
end

-- ############################################################################

function response.read(client)
    local res    = network.read(client)
    local prefix = res:sub(1, -#res)
    local response_handler = protocol.prefixes[prefix]

    if not response_handler then 
        error("Unknown response prefix: " .. prefix)
    else
        return response_handler(client, res)
    end
end

function response.status(client, data)
    local sub = data:sub(2)
    if sub == protocol.ok then return true else return sub end
end

function response.error(client, data)
    local err_line = data:sub(2)

    if err_line:sub(1, 3) == protocol.err then
        error("Redis error: " .. err_line:sub(5))
    else
        error("Redis error: " .. err_line)
    end
end

function response.bulk(client, data)
    local str = data:sub(2)
    local len = tonumber(str)

    if not len then 
        error('Cannot parse ' .. str .. ' as data length.')
    else
        if len == -1 then return nil end
        local next_chunk = network.read(client, len + 2)
        return next_chunk:sub(1, -3);
    end
end

function response.multibulk(client, data)
    local str = data:sub(2)

    -- TODO: add a check if the returned value is indeed a number
    local list_count = tonumber(str)

    if list_count == -1 then 
        return nil
    else
        local list = {}

        if list_count > 0 then 
            for i = 1, list_count do
                table.insert(list, i, response.bulk(client, network.read(client)))
            end
        end

        return list
    end
end

function response.integer(client, data)
    local res = data:sub(2)
    local number = tonumber(res)

    if not number then
        if res == protocol.null then
            return nil
        else
            error('Cannot parse ' .. res .. ' as numeric response.')
        end
    end

    return number
end

protocol.prefixes = {
    ['+'] = response.status, 
    ['-'] = response.error, 
    ['$'] = response.bulk, 
    ['*'] = response.multibulk, 
    [':'] = response.integer, 
}

-- ############################################################################

function request.raw(client, buffer)
    -- TODO: optimize
    local bufferType = type(buffer)

    if bufferType == 'string' then
        network.write(client, buffer)
    elseif bufferType == 'table' then
        network.write(client, table.concat(buffer))
    else
        error('Argument error: ' .. bufferType)
    end

    return response.read(client)
end

function request.inline(client, command, ...)
    if arg.n == 0 then
        network.write(client, command .. protocol.newline)
    else
        local arguments = arg
        arguments.n = nil

        if #arguments > 0 then 
            arguments = table.concat(arguments, ' ')
        else 
            arguments = ''
        end

        network.write(client, command .. ' ' .. arguments .. protocol.newline)
    end

    return response.read(client)
end

function request.bulk(client, command, ...)
    local arguments = arg
    local data      = tostring(table.remove(arguments))
    arguments.n = nil

    -- TODO: optimize
    if #arguments > 0 then 
        arguments = table.concat(arguments, ' ')
    else 
        arguments = ''
    end

    return request.raw(client, { 
        command, ' ', arguments, ' ', #data, protocol.newline, data, protocol.newline 
    })
end

function request.multibulk(client, command, ...)
    local buffer    = { }
    local arguments = { }
    local args_len  = 1

    if arg.n == 1 and type(arg[1]) == 'table' then
        for k, v in pairs(arg[1]) do 
            table.insert(arguments, k)
            table.insert(arguments, v)
            args_len = args_len + 2 
        end
    else
        arguments = arg
        args_len  = args_len + arg.n
        arguments.n = nil
    end
 
    table.insert(buffer, '*' .. tostring(args_len) .. protocol.newline)
    table.insert(buffer, '$' .. #command .. protocol.newline .. command .. protocol.newline)

    for _, argument in pairs(arguments) do
        s_argument = tostring(argument)
        table.insert(buffer, '$' .. #s_argument .. protocol.newline .. s_argument .. protocol.newline)
    end

    return request.raw(client, buffer)
end

-- ############################################################################

local function custom(command, send, parse)
    return function(self, ...)
        local reply = send(self, command, ...)
        if parse then
            return parse(reply, command, ...)
        else
            return reply
        end
    end
end

local function bulk(command, reader)
    return custom(command, request.bulk, reader)
end

local function multibulk(command, reader)
    return custom(command, request.multibulk, reader)
end

local function inline(command, reader)
    return custom(command, request.inline, reader)
end

-- ############################################################################

function connect(...)
    local host, port = defaults.host, defaults.port

    if arg.n == 1 then
        local server = uri.parse(arg[1])
        if server.scheme then
            if server.scheme ~= 'redis' then 
                error('"' .. server.scheme .. '" is an invalid scheme')
            end
            host, port = server.host, server.port or defaults.port
        else
            host, port = server.path, defaults.port
        end
    elseif arg.n > 1 then 
        host, port = arg[1], arg[2]
    end

    if host == nil then 
        error('please specify the address of running redis instance')
    end

    local client_socket = socket.connect(host, tonumber(port))
    if not client_socket then
        error('Could not connect to ' .. host .. ':' .. port)
    end

    local redis_client = {
        socket  = client_socket, 
        raw_cmd = function(self, buffer)
            return request.raw(self, buffer .. protocol.newline)
        end, 
    }

    return load_methods(redis_client, redis_commands)
end

-- ############################################################################

redis_commands = {
    -- miscellaneous commands
    ping  = inline('PING', 
        function(response) 
            if response == 'PONG' then return true else return false end
        end
    ), 
    echo  = bulk('ECHO'),  
    -- TODO: the server returns an empty -ERR on authentication failure
    auth  = inline('AUTH'), 

    -- connection handling
    quit  = custom('QUIT', fire_and_forget), 

    -- commands operating on string values
    set           = bulk('SET'), 
    setnx         = bulk('SETNX', toboolean), 
    mset          = multibulk('MSET'), 
    msetnx        = multibulk('MSETNX', toboolean),  
    get           = inline('GET'), 
    mget          = inline('MGET'), 
    getset        = bulk('GETSET'), 
    incr          = inline('INCR'), 
    incrby        = inline('INCRBY'), 
    decr          = inline('DECR'), 
    decrby        = inline('DECRBY'), 
    exists        = inline('EXISTS', toboolean), 
    del           = inline('DEL', toboolean), 
    type          = inline('TYPE'), 

    -- commands operating on the key space
    keys          = inline('KEYS', 
        function(response) 
            local keys = {}
            response:gsub('[^%s]+', function(key) 
                table.insert(keys, key)
            end)
            return keys
        end
    ),
    randomkey        = inline('RANDOMKEY'), 
    rename           = inline('RENAME'), 
    renamenx         = inline('RENAMENX'), 
    expire           = inline('EXPIRE', toboolean), 
    expireat         = inline('EXPIREAT', toboolean), 
    dbsize           = inline('DBSIZE'), 
    ttl              = inline('TTL'), 

    -- commands operating on lists
    rpush           = bulk('RPUSH'), 
    lpush           = bulk('LPUSH'), 
    llen            = inline('LLEN'), 
    lrange          = inline('LRANGE'), 
    ltrim           = inline('LTRIM'), 
    lindex          = inline('LINDEX'), 
    lset            = bulk('LSET'), 
    lrem            = bulk('LREM'), 
    lpop            = inline('LPOP'), 
    rpop            = inline('RPOP'), 
    rpoplpush       = bulk('RPOPLPUSH'), 

    -- commands operating on sets
    sadd            = bulk('SADD'), 
    srem            = bulk('SREM'), 
    smove           = bulk('SMOVE'), 
    scard           = inline('SCARD'), 
    sismember       = bulk('SISMEMBER'), 
    sinter          = inline('SINTER'), 
    sinterstore     = inline('SINTERSTORE'), 
    sunion          = inline('SUNION'), 
    sunionstore     = inline('SUNIONSTORE'), 
    sdiff           = inline('SDIFF'), 
    sdiffstore      = inline('SDIFFSTORE'), 
    smembers        = inline('SMEMBERS'), 
    srandmember     = inline('SRANDMEMBER'), 

    -- commands operating on sorted sets 
    zadd            = bulk('ZADD'), 
    zincrby         = bulk('ZINCRBY'), 
    zrem            = bulk('ZREM'), 
    zrange          = inline('ZRANGE'), 
    zrangebyscore   = inline('ZRANGEBYSCORE'), 
    zrevrange       = inline('ZREVRANGE'), 
    zcard           = inline('ZCARD'), 
    zscore          = bulk('ZSCORE'), 
    zremrangebyscore = inline('ZREMRANGEBYSCORE'), 
    zrank           = bulk('ZRANK'),
    zrevrank        = bulk('ZREVRANK'),
    zremrangebyrank = inline('ZREMRANGEBYRANK'),
    
    -- commands operating on hashes



    -- multiple databases handling commands
    select         = inline('SELECT'), 
    move           = inline('MOVE'), 
    flushdb        = inline('FLUSHDB'), 
    flushall       = inline('FLUSHALL'), 

    -- sorting
    --[[ params = { 
            by    = 'weight_*', 
            get   = 'object_*', 
            limit = { 0, 10 },
            sort  = 'desc',
            alpha = true, 
        }   
    --]]
    sort  = custom('SORT', 
        function(client, command, key, params)
            local query = { key }

            if params then
                if params.by then 
                    table.insert(query, 'BY ' .. params.by)
                end

                if type(params.limit) == 'table' then 
                    -- TODO: check for lower and upper limits
                    table.insert(query, 'LIMIT ' .. params.limit[1] .. ' ' .. params.limit[2])
                end

                if params.get then 
                    table.insert(query, 'GET ' .. params.get)
                end

                if params.sort then
                    table.insert(query, params.sort)
                end

                if params.alpha == true then
                    table.insert(query, 'ALPHA')
                end

                if params.store then
                    table.insert(query, 'STORE ' .. params.store)
                end
            end

            return request.inline(client, command, table.concat(query, ' '))
        end
    ), 

    -- persistence control commands
    save            = inline('SAVE'), 
    bgsave          = inline('BGSAVE'), 
    lastsave        = inline('LASTSAVE'), 

    -- remote server control commands
    info = inline('INFO', 
        function(response) 
            local info = {}
            response:gsub('([^\r\n]*)\r\n', function(kv) 
                local k,v = kv:match(('([^:]*):([^:]*)'):rep(1))
                info[k] = v
            end)
            return info
        end
    ),
}
