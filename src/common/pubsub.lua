local skynet = require "skynet"
local logger = require "logger"
local service = ".redis_pubsub"
local futil = require "futil"
local pubsub = {}
local mt = {}
mt.__index = function(t, k) 
	local f = function(self, ...)
		return skynet.call(service, 'lua', 'command', k, ...) 
	end
	return f
end
setmetatable(pubsub, mt)

function pubsub.sub(channel, hook)
	if not service then 
        logger.err("sub fail: service nil")
        return false 
    end
	local ok, err = xpcall(skynet.call, futil.handle_err, service, 'lua', 'sub', channel, skynet.self(), hook)
    if not ok then
        logger.debug("sub:%s,%s", ok, tostring(err))
    end
    return ok
end

function pubsub.pub(channel, msg)
	if not service then return false end
	return pcall(skynet.call, service, 'lua', 'pub', channel, msg)
end

skynet.init(function()
end)

return pubsub
