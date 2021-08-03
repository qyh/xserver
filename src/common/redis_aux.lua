local skynet = require "skynet"
local logger = require "logger"
local futil = require "futil"
local redis_aux = {}
local service = ".redis_service"
local mt = {}
local redis_id = nil
mt.__index = function(t, k) 
    if not redis_id then
        logger.err("no redis_id selected")
        return false
    end
    local f = function(self, ...)
        return skynet.call(service, 'lua', 'command', redis_id, k, ...) 
    end
    return f
end
setmetatable(redis_aux, mt)

skynet.init(function()
end)

function redis_aux.db(id) 
    redis_id = id
    return redis_aux 
end

return redis_aux
