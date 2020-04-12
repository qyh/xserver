local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
local function boot()
	skynet.newservice("logservice")
    skynet.newservice('webclient')
    skynet.newservice('payment')
	skynet.uniqueservice('snowflake')
    --skynet.newservice("trans_service")
    skynet.uniqueservice('redis_pubsub')    
    skynet.uniqueservice('mt_lock_service')
    --skynet.newservice("mysql_service")
    local x = skynet.newservice("xservice")
    local job = skynet.getenv("job_name")
    logger.debug("get job:%s", job)
    skynet.call(x, "lua", job)
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
end
skynet.start(function()
	local ok, res = xpcall(boot, futil.handle_err)
	if not ok then
		skynet.error(string.format("boot fail::%s", tostring(res)))
	else
		skynet.error("boot success !")
	end
end)
