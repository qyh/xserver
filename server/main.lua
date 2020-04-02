local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
skynet.start(function()
    --skynet.newservice("logservice")
    skynet.newservice('webclient')
    skynet.newservice('payment')
	skynet.uniqueservice('snowflake')
    --skynet.newservice("trans_service")
	local redis_conf = skynet.getenv("redis_conf") 
	local conf = json.decode(redis_conf)
    skynet.uniqueservice('redis_pubsub')    
	logger.debug("redis_conf:%s", futil.toStr(conf))
	local mysql_conf = skynet.getenv("mysql_conf")
	conf = json.decode(mysql_conf)
	logger.debug("mysql_conf:%s", futil.toStr(conf))
    logger.debug("db.db:%s", futil.toStr(dbconf))
    --skynet.newservice("mysql_service")
    skynet.newservice("cron_service")
    --[[
    local rpc = skynet.newservice("rpc_service")
    local ok = skynet.call(rpc, "lua", "start", {
        port = 50600,
        maxclient = 10000,
        nodelay = true
    })
    logger.info('rpc start :%s', ok)
    ok = skynet.call(rpc, "lua", "connect", {
        host = "127.0.0.1",
        port = 50600
    })
    logger.info('rpc connect:%s', ok)
    ]]
end)
