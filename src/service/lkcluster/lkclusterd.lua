local skynet = require "skynet"
local sc = require "skynet.socketchannel"
local socket = require "skynet.socket"
local cluster = require "skynet.cluster.core"
local lkmonitor_handler = require "lkmonitor_handler"
local futil = require "futil"

local config_name = skynet.getenv "cluster"
local node_address = {}
local node_session = {}
local node_f2s = {}
local node_s2f = {}
local command = {}

local function read_response(sock)
	local sz = socket.header(sock:read(2))
	local msg = sock:read(sz)
	return cluster.unpackresponse(msg)	-- session, ok, data, padding
end

local function open_channel(t, key)
	local host, port = string.match(node_address[key], "([^:]+):(.*)$")
	local c = sc.channel {
		host = host,
		port = tonumber(port),
		response = read_response,
		nodelay = true,
	}
	assert(c:connect(true))
	t[key] = c
	return c
end

local node_channel = setmetatable({}, { __index = open_channel })

local function clear_channel(name)
	local c = rawget(node_channel, name)
	if c then
		node_channel[name] = nil
		pcall(c.close, c)
		return true
	end
end

local function loadconfig(config)
	if not config then return end
	for fullname,address in pairs(config) do
		assert(type(address) == "string")
		local name = string.match(fullname, "^[^#]+")
		if node_address[name] ~= address then
			-- address changed
			clear_channel(name)
			node_address[name] = address
		end
		local old = node_s2f[name]
		if old ~= fullname then
			if old then node_f2s[old] = nil end
			node_s2f[name] = fullname
			node_f2s[fullname] = name
		end
	end
	-- clean nodes not in config
	for name,fullname in pairs(node_s2f) do
		if not config[fullname] then
			skynet.error(string.format("lkclusterd node removed: name=%s fullname=%s address=%s",
				name, fullname, node_address[name]))
			clear_channel(name)
			node_address[name] = nil
			node_s2f[name] = nil
			node_f2s[fullname] = nil
		end
	end
end

function command.reload(source, config)
	loadconfig(config)
	skynet.ret(skynet.pack(nil))
end

function command.listen(source, addr, port)
	local gate = skynet.newservice("gate")
	if port == nil then
		addr, port = string.match(node_address[addr], "([^:]+):(.*)$")
	end
	skynet.call(gate, "lua", "open", { address = addr, port = port })
	skynet.ret(skynet.pack(nil))
end

local function resolve_node(node)
	if node_address[node] then return node end
	return assert(node_f2s[node], "expired version node name: "..tostring(node))
end

local function send_request(no_response, source, node, addr, msg, sz)
	node = resolve_node(node)
	local session = node_session[node] or 1
	local dsession = no_response and session * 2 or session * 2 -1
	-- msg is a local pointer, cluster.packrequest will free it
	local request, _, padding = cluster.packrequest(addr, dsession, msg, sz)
	node_session[node] = session < 0x3fffffff and session + 1 or 1

	-- node_channel[node] may yield or throw error
	local c = node_channel[node]

	if no_response then dsession = nil end
	return c:request(request, dsession, padding)
end

function command.req(...)
	local ok, msg, sz = xpcall(send_request, futil.handle_err, false, ...)
	if ok then
		if type(msg) == "table" then
			skynet.ret(cluster.concat(msg))
		else
			skynet.ret(msg)
		end
	else
		local source, node, addr = ...
		skynet.error(string.format("lkcluster error: node = %s, addr = %s, err = %s", node, addr, msg))
		-- skynet.error(msg)
		skynet.response()(false)
	end
end

function command.send(...)
	local ok, err = pcall(send_request, true, ...)
	if not ok then
		local source, node, addr = ...
		skynet.error(string.format("lkcluster error: node = %s, addr = %s, err = %s", node, addr, err))
	end
end

function command.reset(source, node)
	local ok, node = pcall(resolve_node, node)
	if ok and node and clear_channel(node) then
		skynet.error("lkclusterd reset", node)
		return true
	end
	return false
end

function command.block_reset(...)
	skynet.ret(command.reset(...))
end

local proxy = {}

function command.proxy(source, node, name)
	node = resolve_node(node)
	local fullname = node .. "." .. name
	if proxy[fullname] == nil then
		proxy[fullname] = skynet.newservice("lkclusterproxy", node, name)
	end
	skynet.ret(skynet.pack(proxy[fullname]))
end

local register_name = {}

function command.register(source, name, addr)
	assert(register_name[name] == nil)
	addr = addr or source
	local old_name = register_name[addr]
	if old_name then
		register_name[old_name] = nil
	end
	register_name[addr] = name
	register_name[name] = addr
	skynet.ret(nil)
	skynet.error(string.format("Register [%s] :%08x", name, addr))
end

local large_request = {}

function command.socket(source, subcmd, fd, msg)
	if subcmd == "data" then
		local sz
		local addr, session, msg, padding = cluster.unpackrequest(msg)
		if padding then
			local req = large_request[session] or { addr = addr }
			large_request[session] = req
			table.insert(req, msg)
			return
		end
		local req = large_request[session]
		if req then
			large_request[session] = nil
			table.insert(req, msg)
			msg,sz = cluster.concat(req)
			addr = req.addr
		end
		local no_response = (session&1) == 0
		if not msg then
			if not no_response then
				local response = cluster.packresponse(session, false, "Invalid large req")
				socket.write(fd, response)
			end
			return
		end
		local ok, response
		if addr == 0 then
			if no_response then return end
			local name = skynet.unpack(msg, sz)
			local addr = register_name[name]
			if addr then
				ok = true
				msg, sz = skynet.pack(addr)
			else
				ok = false
				msg = "name not found"
			end
		else
			if no_response then
				return skynet.rawsend(addr, "lua", msg, sz)
			end
			ok, msg, sz = pcall(lkmonitor_handler.rawcall, addr, "lua", msg, sz)
		end
		if ok then
			response = cluster.packresponse(session, true, msg, sz)
			if type(response) == "table" then
				for _, v in ipairs(response) do
					socket.lwrite(fd, v)
				end
			else
				socket.write(fd, response)
			end
		else
			response = cluster.packresponse(session, false, msg)
			socket.write(fd, response)
		end
	elseif subcmd == "open" then
		skynet.error(string.format("socket accept from %s, fd = %s", msg, fd))
		skynet.call(source, "lua", "accept", fd)
	else
		large_request = {}
		skynet.error(string.format("socket %s %d : %s", subcmd, fd, msg))
	end
end

skynet.start(function()
	skynet.dispatch("lua", function(session , source, cmd, ...)
		local f = assert(command[cmd])
		f(source, ...)
	end)
	skynet.info_func(function ()
		local strTbl = {}
		for k,v in pairs(node_address) do
			local c = rawget(node_channel, k)
			table.insert(strTbl, string.format("nodename=%s, address=%s, socket=%s",
				node_s2f[k] or k, v, c and c.__sock and c.__sock[1]))
		end
		table.sort(strTbl)
		return table.concat(strTbl, "\n")
	end)
end)
