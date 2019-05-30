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
    --skynet.newservice('websocket')
    --skynet.newservice('webserver')
end)
