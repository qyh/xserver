local skynet = require "skynet"
local cluster = require "xcluster"
local logger = require "logger"
local mc = {
    register = cluster.register
}

local function timeout_call(timeout, nodetype, ...) 
    local curtime = os.time()
    local node = skynet.call(".clustermgr", "lua", "query_node", nodetype)
    while not node do
        node = skynet.call(".clustermgr", "lua", "query_node", nodetype)
        if node then
            break
        end
        if os.time() - curtime > timeout then
            break
        end
        skynet.sleep(1)
    end
    if not node then
        return false, string.format("nodetype:%s empty", nodetype)
    end
    return cluster.call(node, ...)
end
local function timeout_send(timeout, nodetype, ...) 
    local curtime = os.time()
    local node = skynet.call(".clustermgr", "lua", "query_node", nodetype)
    while not node do
        node = skynet.call(".clustermgr", "lua", "query_node", nodetype)
        if node then
            break
        end
        if os.time() - curtime > timeout then
            break
        end
        skynet.sleep(1)
    end
    if not node then
        return false, string.format("nodetype:%s empty", nodetype)
    end
    return cluster.send(node, ...)
end

function mc.call(nodetype, ...) 
    return timeout_call(30, nodetype, ...) 
end

function mc.send(nodetype, ...)
    return timeout_send(30, nodetype, ...) 
end

return mc
