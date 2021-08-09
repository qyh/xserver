local skynet = require "skynet"
local cluster = require "lkcluster"

local mc = {
    register = cluster.register,
}

function mc.query(node, name)
    local ok, r = pcall(cluster.query, node, name)
    if ok then
        return r
    else
        skynet.error(string.format("query %s:%s fail", node, name))
        return nil
    end
end

function mc.send(node, name, ...)
    if type(name) == "string" then
        local addr = mc.query(node, name)
        if not addr then
            return false
        end
        return cluster.send(node, addr, ...)
    else
        return cluster.send(node, name, ...)
    end
end

function mc.call(node, name, ...)
    if type(name) == "string" then
        local addr = mc.query(node, name)
        if not addr then
            return false
        end
        return cluster.call(node, addr, ...)
    else
        return cluster.call(node, name, ...)
    end
end

function mc.get(name)
	return skynet.call(".clustermgr", "lua", "get", name)
end

function mc.set(name, address)
	return skynet.call(".clustermgr", "lua", "set", name, address)
end

function mc.names(pat, except_self)
	return skynet.call(".clustermgr", "lua", "names", pat, except_self)
end

function mc.short_names(pat, except_self)
	return skynet.call(".clustermgr", "lua", "short_names", pat, except_self)
end

local function do_broadcast(pat, address, ...)
	return cluster.broadcast(mc.names(pat), address, ...)
end

function mc.broadcast(pat, address, ...)
	return skynet.fork(do_broadcast, pat, address, ...)
end

function mc.mcall(pat, address, ...)
	return cluster.mcall(mc.names(pat), address, ...)
end

local function do_broadcast_others(pat, address, ...)
	return cluster.broadcast(mc.names(pat, true), address, ...)
end

function mc.broadcast_others(pat, address, ...)
	return skynet.fork(do_broadcast_others, pat, address, ...)
end

function mc.mcall_others(pat, address, ...)
	return cluster.mcall(mc.names(pat, true), address, ...)	
end

return mc
