local skynet = require "skynet"
local redis = require "skynet.db.redis"
local json = require "cjson"
local logger = require "logger"
local futil = require "futil"
local const = require "const"
local mt_lock = require "mt_lock"
require "tostring"
require "skynet.manager"
local CMD = {}

function CMD.audit_mt_lock(...)
    logger.debug("audit_mt_lock")
    for i=1, 10 do
        skynet.fork(function()
            local k = string.format("locktest")
            logger.debug("%i try get lock %s", i, k)
            local ok, timeout = mt_lock.lock_wait(k, i, 30)
            logger.debug("%i get lock %s success", i, k)
            mt_lock.unlock(k, i)
            logger.debug("%i unlock %s", i, k)
        end)
    end
    logger.debug("audit_mt_lock end")
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
    skynet.register(".xservice")
end)

