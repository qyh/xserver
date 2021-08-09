local skynet = require "skynet"
require "skynet.manager"
local skynet_util = require "skynet_util"
local logger = require "logger"

-- It's a simple service exit monitor, you can do something more when a service exit.

local service_map = {}
local service_invalid = {}
local CMD = {}

local cache_expire_time = 86400*3*100

--- clean expire invalid cache
local function clean_invalid()
	local interval = cache_expire_time
	local now = skynet.now()
	for service, t in pairs(service_invalid) do
		local left_t = cache_expire_time - (now - t)
		if left_t <= 0 then
			service_invalid[service] = nil
		elseif left_t < interval then
			interval = left_t
		end
	end
	return interval
end

local function clean_invalid_main()
	while true do
		local interval = clean_invalid()
		skynet.sleep(interval)
	end
end

function CMD.watch(watcher, service)
	service = math.tointeger(service) or skynet.localname(service)
	if not service then return false end
	local w = service_map[service]
	if not w then
		if service_invalid[service] then return false end
		if not skynet.send(service, "debug", "LINK") then
			service_invalid[service] = skynet.now()
			return false
		end
		w = {}
		service_map[service] = w
	end
	w[watcher] = true
	skynet.error(string.format("lkmonitor watch, watcher: %d, service: %d", watcher, service))
	return true
end

function CMD.unwatch(watcher, service)
	service = math.tointeger(service) or skynet.localname(service)
	if not service then return end
	local w = service_map[service]
	if not w then return end
	w[watcher] = nil
end

skynet.register_protocol {
	name = "client",
	id = skynet.PTYPE_CLIENT,	-- PTYPE_CLIENT = 3
	unpack = function() end,
	dispatch = function(_, address)
		local w = service_map[address]
		if w then
			for watcher in pairs(w) do
				skynet.redirect(watcher, address, "error", 0, "")
			end
			service_map[address] = nil
			service_invalid[address] = skynet.now()
		end
	end
}

skynet.start(function()
	skynet.dispatch("lua", function(session, source, cmd, ...)
        return skynet_util.lua_docmd(CMD, session, cmd, source, ...)
   end)
	skynet.fork(clean_invalid_main)
	skynet.register(".monitor")
end)
