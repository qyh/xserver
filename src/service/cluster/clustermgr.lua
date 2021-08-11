local skynet = require "skynet"
local cluster = require "xcluster"
local redis = require "skynet.db.redis"
local json = require "cjson"
local logger = require "logger"
local futil = require "futil"
local const = require "const"
local dbconf = require "db.db"
local redis_config = nil
require "tostring"
require "skynet.manager"
local conf_name = "single"
local db = nil
-- node_config: {[nodetype] = {"db" = "127.0.0.1:2529"}}
local node_config = {} 
local all_conf = {}
for k, v in pairs(dbconf.redis) do
    if conf_name == k then
        redis_config = v
    end
end

local wait_cos = {}

local CMD = {}
local clusterkey = "cluster_config"
local nodename = skynet.getenv("nodename")
local nodeip = skynet.getenv("nodeip")
local nodeport = tonumber(skynet.getenv("nodeport"))
local nodetype = tonumber(skynet.getenv("nodetype"))
local node_balance = {}

local function load_config()
    if not db then
        return {}
    end
    --[[
    {
        db = "127.0.0.1:2529",
        db2 = "127.0.0.1:2528",
    }
    ]]
    local conf = {}
    local res = db:hgetall(clusterkey)
    local curtime = os.time()
    if res then
        for k=1, #res, 2 do
            local nodename = res[k]
            local jstr = res[k+1]
            local ok, obj = pcall(json.decode, jstr)
            if ok and obj and obj.nodetype and obj.nodeip and obj.nodeport and curtime - tonumber(obj.updatedAt) < 10 then
                local nodes = node_config[obj.nodetype] or {}
                nodes[nodename] = string.format("%s:%d",obj.nodeip, obj.nodeport)
                conf[nodename] = nodes[nodename]
                node_config[obj.nodetype] = nodes
            else
                logger.warn("node:%s expire: %s %s", nodename, obj.updatedAt, curtime)
                db:hdel(clusterkey, nodename)
            end
        end
    else
        return conf
    end
    return conf
end

skynet.init(function()

end)

local function reload()
    local tmp_conf = load_config()
    for k, v in pairs(all_conf) do
        if not tmp_conf[k] then
            tmp_conf[k] = false -- node is down
        end
    end
    cluster.reload(tmp_conf)
    all_conf = tmp_conf
end

local function update_nodes()
    local obj = {
        nodename = nodename,
        nodeip = nodeip,
        nodetype = nodetype,
        nodeport = nodeport,
        updatedAt = os.time()
    }
    local ok = db:hset(clusterkey, nodename, json.encode(obj))
    reload()
    skynet.timeout(500, update_nodes)
end

local function bracast_self()
    local tmp_conf = all_conf
    for k,v in pairs(tmp_conf) do
        if k ~= nodename then
            local obj = {
                nodename = nodename,
                nodeip = nodeip,
                nodetype = nodetype,
                nodeport = nodeport,
                updatedAt = os.time()
            }
            cluster.send(k, "@clustermgr", "add_node", nodename, obj) 
        end
    end
end

local function connect_redis()
    if redis_config then
        db = redis.connect(redis_config)
        if db then
            logger.debug('redis connect success')
        else
            logger.err('redis connect failed:%s', table.tostring(redis_config))
        end
    else
        logger.err("redis_config nil, exist")
        return false
    end
    return true
end

function CMD.add_node(node, obj)
    logger.info("on add_node:%s,%s", node, json.encode(obj))
    local addr = obj.nodeip..":"..obj.nodeport
    local nodes = node_config[obj.nodetype] or {}
    nodes[node] = addr
    node_config[obj.nodetype] = nodes
    all_conf[node] = addr
    cluster.reload(all_conf)
    for k, v in pairs(wait_cos) do
        if v.nodetype == obj.nodetype then
            local item = table.remove(wait_cos, k)
            if item and next(item) then
                logger.warn("add_node wakeup co :%s", tostring(item))
                skynet.wakeup(item.co)
            end
        end
    end
end

local function get_node(nodetype)
    local nodes = node_config[nodetype] or {}
    if not next(nodes) then
        return false, ""
    end
    local nodenames = {}
    for k, v in pairs(nodes) do
        table.insert(nodenames, k)
    end
    local balance = node_balance[nodetype] or 1
    node_balance[nodetype] = balance + 1 > #nodenames and 1 or balance + 1
    return true, nodenames[balance]
end

function CMD.query_node(nodetype, timeout)
    timeout = timeout or 0
    local ok, node = get_node(nodetype)
    if not ok then
        if timeout > 0 then
            local co = coroutine.running()
            table.insert(wait_cos, {
                co = co,
                nodetype = nodetype,
                timeout = os.time() + timeout,
            })
            skynet.wait(co)
            return get_node(nodetype)
        end
    end
    return ok, node
end

local function wakeup_cos()
    local curtime = os.time()
    for k, v in pairs(wait_cos) do
        if curtime > v.timeout then
            local item = table.remove(wait_cos, k)
            if item and next(item) then
                logger.warn("timeout wakeup co :%s", tostring(item.co))
                skynet.wakeup(item.co)
            end
        end
    end
    skynet.timeout(100, wakeup_cos)
end

skynet.start(function()
    skynet.dispatch('lua', function(session, address, cmd, ...)
        local f = CMD[cmd]
        if f then
            if session > 0 then
                skynet.ret(skynet.pack(f(...)))
            else
                f(...)
            end
        else
            logger.err('ERROR: Unknown command:%s', tostring(cmd))
        end
    end)
    if not connect_redis() then
        skynet.exit()
    end
    update_nodes()
    cluster.open(nodename)
    skynet.register(".clustermgr")
    cluster.register("clustermgr")
    bracast_self() 
    wakeup_cos()
end)

