local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local function boot()
    skynet.newservice("logservice")
    skynet.newservice("clustermgr")
    skynet.newservice("dispatcher")
    skynet.newservice("room")
    logger.info("room start success")
end
skynet.start(function()
    local ok, res = xpcall(boot, futil.handle_err)
    if not ok then
        skynet.error(string.format("boot fail::%s", tostring(res)))
    else
        skynet.error("boot success !")
    end
end)
