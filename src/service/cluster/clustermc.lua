local skynet = require "skynet"
local cluster = require "xcluster"
local logger = require "logger"
local mc = {
    register = cluster.register
}

function mc.call(nodetype, ...) 
    local node = skynet.call(".clustermgr", "lua", "query_node", nodetype)
    if not node then
        return false, string.format("nodetype:%s empty", nodetype)
    end
    return cluster.call(node, ...)
end

function mc.send(nodetype, ...)
    local node = skynet.call(".clustermgr", "lua", "query_node", nodetype)
    if not node then
        return false, string.format("nodetype:%s empty", nodetype)
    end
    return cluster.send(node, ...)
end

return mc
