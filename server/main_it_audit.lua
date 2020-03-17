local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
skynet.start(function()
    local debug_port = skynet.getenv("debug_port")
    if tonumber(debug_port) then
        skynet.newservice("debug_console", debug_port)
    end
    skynet.newservice("logservice")
	skynet.uniqueservice('snowflake')
	skynet.uniqueservice('redis_pubsub')	
    logger.debug("db.db:%s", futil.toStr(dbconf))
    skynet.newservice("mysql_service")
    skynet.newservice("audit_service")
    
end)
