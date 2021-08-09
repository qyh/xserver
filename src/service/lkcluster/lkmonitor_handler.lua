local skynet = require "skynet"
require "skynet.manager"
local handler = {}
local monitor_cache = {}
local lkmonitor

local cache_expire_time = 86400*3*100

--- clean expire cache
local function clean_cache()
	local interval = cache_expire_time
	local now = skynet.now()
	for addr, t in pairs(monitor_cache) do
		local left_t = cache_expire_time - (now - t)
		if left_t <= 0 then
			monitor_cache[addr] = nil
		elseif left_t < interval then
			interval = left_t
		end
	end
	return interval
end

local function clean_main()
	while true do
		local interval = clean_cache()
		skynet.sleep(interval)
	end
end

local function watch(service)
	local addr = math.tointeger(service) or skynet.localname(service)
	if not addr then
		error("lkmonitor_handler watch invalid service: " .. tostring(service))
	end
	if not monitor_cache[addr] then
		if not lkmonitor then
			lkmonitor = skynet.monitor("lkmonitor")
		end
		local ok = skynet.call(lkmonitor, "lua", "watch", addr)
		if not ok then
			error(string.format("lkmonitor_handler watch fail, service: %s, addr: %d", service, addr))
		end
		skynet.error(string.format("lkmonitor_handler watch success, service: %s, addr: %d", service, addr))
		monitor_cache[addr] = skynet.now()
	end
	return addr
end

function handler.watch(service)
	return watch(service)
end

function handler.call(service, ...)
	return skynet.call(watch(service), ...)
end

function handler.rawcall(service, ...)
	return skynet.rawcall(watch(service), ...)
end

function handler.unwatch(service)
	local addr = math.tointeger(service) or skynet.localname(service)
	if not addr then return end
	if not monitor_cache[addr] then return end
	return skynet.call(lkmonitor, "lua", "unwatch", addr)
end

function handler.clean(service)
	local addr = math.tointeger(service) or skynet.localname(service)
	if not addr then return end
	monitor_cache[addr] = nil
end

skynet.init(function ()
	skynet.fork(clean_main)
end)

return handler
