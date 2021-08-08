local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local clustermc = require "clustermc"
local function boot()
    skynet.newservice("logservice")
    skynet.newservice("clustermgr") 
    local v = clustermc.call(1, "sdb", "GET", "a")
    logger.info("get a :%s", v)
end
skynet.start(function()
    local ok, res = xpcall(boot, futil.handle_err)
    if not ok then
        skynet.error(string.format("boot fail::%s", tostring(res)))
    else
        skynet.error("boot success !")
    end
end)
