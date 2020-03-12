local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
skynet.start(function()
    skynet.newservice("logservice")
	skynet.uniqueservice('snowflake')
    logger.debug("db.db:%s", futil.toStr(dbconf))
    skynet.newservice("mysql_service")
    skynet.newservice("audit_service")
    
end)
