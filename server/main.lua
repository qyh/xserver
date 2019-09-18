local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
skynet.start(function()
	--[[
	local loginserver = skynet.newservice("logind")
	local gate = skynet.newservice("gated", loginserver)

	skynet.call(gate, "lua", "open" , {
		port = 6174,
		maxclient = 1024,
		servername = "login_server",
	})
	]]
    --logger.err("%s","start login server ...")
    --skynet.newservice('logger')
    skynet.newservice("logservice")
    skynet.newservice('webclient')
    skynet.newservice('payment')
    skynet.newservice('simpleweb')
	skynet.uniqueservice('snowflake')
	local redis_conf = skynet.getenv("redis_conf") 
	local conf = json.decode(redis_conf)
	logger.debug("redis_conf:%s", futil.toStr(conf))
	local mysql_conf = skynet.getenv("mysql_conf")
	conf = json.decode(mysql_conf)
	logger.debug("mysql_conf:%s", futil.toStr(conf))
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
    --skynet.newservice('websocket')
    --skynet.newservice('webserver')
end)
