local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
local name = skynet.getenv("name")
local function boot()
    skynet.newservice("logservice")
    skynet.newservice('webclient')
    skynet.newservice('payment')
    skynet.uniqueservice('snowflake')
    --skynet.newservice("trans_service")
    skynet.uniqueservice('redis_pubsub')    
    skynet.uniqueservice('mt_lock_service')
    skynet.newservice("mysql_service")
    local x = skynet.newservice("xservice")
    local job = skynet.getenv("job_name")
    logger.debug("get job:%s", job)
    skynet.call(x, "lua", job)
end
skynet.start(function()
    local ok, res = xpcall(boot, futil.handle_err)
    if not ok then
        skynet.error(string.format("%s boot fail::%s", name, tostring(res)))
    else
        skynet.error(name .. "boot success !")
    end
end)
