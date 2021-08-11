local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local function boot()
    skynet.newservice("logservice")
    skynet.newservice("clustermgr")
    local w = skynet.newservice("xwatchdog")
    local ok = skynet.call(w, "lua", "start", {
        port = 50600,
        maxclient = 10000,
        nodelay = true
    })
    
    logger.info('connector start : %s', ok)
end
skynet.start(function()
    local ok, res = xpcall(boot, futil.handle_err)
    if not ok then
        skynet.error(string.format("boot fail::%s", tostring(res)))
    else
        skynet.error("boot success !")
    end
end)
