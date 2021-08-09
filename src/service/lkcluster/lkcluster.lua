local skynet = require "skynet"
local futil = require "futil"
local logger = require "logger"

local clusterd
local cluster = {}
local nodeName
local nodeFullName

function cluster.rawcall(node, address, ...)
	if node == nodeFullName or node == nodeName then
		return table.pack(pcall(skynet.call, address, "lua", ...))
	end
	local self_co = coroutine.running()
	local r
	skynet.fork(function (...)
		r = table.pack(pcall(skynet.call, clusterd, "lua", "req", node, address, ...))
		if not self_co then return end
		-- if not r[1] then
		-- 	logger.warn("cluster call fail, node [%s] address [%s] param1 [%s], error: %s",
		-- 		node, address, tostring(...), r[2])
		-- end
		skynet.wakeup(self_co)
		self_co = nil
	end, skynet.pack(...))	-- NOTE: pack parameters, so they can be modified outer
	skynet.timeout(1000, function ()
		if not self_co then return end
		-- logger.warn("cluster call timeout, node [%s] address [%s], \r\n%s",
		-- 	node, address, debug.traceback(self_co))
		skynet.wakeup(self_co)
		self_co = nil
		skynet.send(clusterd, "lua", "reset", node)
	end)
	skynet.wait()
	return r
end

-- NOTICE:
-- call directly, no need pcall
-- different from origin cluster implement, first return value is skynet.call ok or not
function cluster.call(node, address, ...)
	-- skynet.pack(...) will free by cluster.core.packrequest
	local r = cluster.rawcall(node, address, ...)
	if not r then
		return false, "timeout"
	end
	return table.unpack(r, 1, r.n)
end

function cluster.send(node, address, ...)
	if node == nodeFullName or node == nodeName then
		return skynet.send(address, "lua", ...)
	end
	return skynet.send(clusterd, "lua", "send", node, address, skynet.pack(...))
end
function cluster.query(node, name)
	return skynet.call(clusterd, "lua", "req", node, 0, skynet.pack(name))
end

function cluster.broadcast(names, address, ...)
	if not names or #names == 0 then return end
	for _, node in ipairs(names) do
		cluster.send(node, address, ...)
	end
end

function cluster.mcall(names, address, ...)
	if not names or #names == 0 then return end
	local count = #names
	local self_co = coroutine.running()
	local results = {}
	for _, node in ipairs(names) do
		skynet.fork(function (...)
			results[node] = cluster.rawcall(node, address, ...) or {false, "timeout", n=2}
			if count == 1 then skynet.wakeup(self_co) else count = count - 1 end
		end, ...)
	end
	skynet.wait()
	return results
end

function cluster.open(port)
	if type(port) == "string" then
		skynet.call(clusterd, "lua", "listen", port)
	else
		skynet.call(clusterd, "lua", "listen", "0.0.0.0", port)
	end
end

function cluster.reload(config)
	skynet.call(clusterd, "lua", "reload", config)
end

function cluster.proxy(node, name)
	return skynet.call(clusterd, "lua", "proxy", node, name)
end
function cluster.register(name, addr)
	assert(type(name) == "string")
	assert(addr == nil or type(addr) == "number")
	return skynet.call(clusterd, "lua", "register", name, addr)
end

function cluster.snax(node, name, address)
	local snax = require "snax"
	if not address then
		address = cluster.call(node, ".service", "QUERY", "snaxd" , name)
	end
	local handle = skynet.call(clusterd, "lua", "proxy", node, address)
	return snax.bind(handle, name)
end

skynet.init(function()
	clusterd = skynet.uniqueservice("lkclusterd")
	nodeName = skynet.getenv("nodename")
	nodeFullName = skynet.getenv("nodeFullName")
end)

return cluster
