local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local function boot()
    skynet.newservice("logservice")
    skynet.newservice("connector_agent")
    local conn = skynet.newservice("connector")
    local ok = skynet.call(conn, "lua", "open", {
        port = 50600,
        maxclient = 10000,
        nodelay = true
    })
    
    logger.info('connector start : %s', ok)
    local cli = skynet.newservice("connector_client")
    local conn = skynet.call(cli, "lua", "connect", {
        host = "127.0.0.1",
        port = 50600
    })
    logger.info("conn:", conn) 
end
skynet.start(function()
    local ok, res = xpcall(boot, futil.handle_err)
    if not ok then
        skynet.error(string.format("boot fail::%s", tostring(res)))
    else
        skynet.error("boot success !")
    end
end)
