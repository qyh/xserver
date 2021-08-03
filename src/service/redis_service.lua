local skynet = require "skynet"
local redis = require "skynet.db.redis"
local json = require "cjson"
local logger = require "logger"
local futil = require "futil"
local const = require "const"
local db_conf = require "db.db"
local redis_conf = db_conf.redis 
require "tostring"
require "skynet.manager"
local CMD = {}
local dbs = {}

function CMD.command(db_id, cmd, ...)
    local db = dbs[db_id]
    if db then
        local f = db[cmd]
        if f then
            return f(db, ...)
        end
    end
    return nil
end



skynet.init(function()
    
end)

skynet.start(function()
    skynet.dispatch('lua', function(session, address, cmd, ...)
        local f = CMD[cmd]
        if f then
            if session > 0 then
                skynet.ret(skynet.pack(f(...)))
            else
                f(...)
            end
        else
            logger.err('ERROR: Unknown command:%s', tostring(cmd))
        end
    end)
    for k, v in pairs(redis_conf) do
        local db = redis.connect(v)
        if db then
            logger.debug('redis connect success %s: %s', k, table.tostring(v))
            dbs[k] = db
        else
            logger.err('redis connect fail %s: %s', k, table.tostring(v))
        end
    end
    skynet.register(".redis_service")
end)

