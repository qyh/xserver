local skynet = require "skynet"
local cluster = require "xcluster"
local logger = require "logger"
local mc = {
    register = cluster.register
}

local function timeout_call(timeout, nodetype, ...) 
    local ok, node = skynet.call(".clustermgr", "lua", "query_node", nodetype, timeout)
    if not ok then
        return false, string.format("nodetype:%s empty", nodetype)
    else
        return true, cluster.call(node, ...)
    end
end
local function timeout_send(timeout, nodetype, ...) 
    local ok, node = skynet.call(".clustermgr", "lua", "query_node", nodetype, timeout)
    if not ok then
        return false, string.format("nodetype:%s empty", nodetype)
    else
        return true, cluster.send(node, ...)
    end
end

function mc.call(nodetype, ...) 
    return timeout_call(10, nodetype, ...) 
end

function mc.send(nodetype, ...)
    return timeout_send(10, nodetype, ...) 
end

function mc.call_node(nodename, ...)
    return pcall(cluster.call, nodename, ...)
end

return mc
