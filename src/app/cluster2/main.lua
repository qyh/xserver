local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local clustermc = require "clustermc"
local function boot()
    skynet.newservice("logservice")
    skynet.newservice("clustermgr") 
    skynet.timeout(200, function()
        --调用远程服务要在服务名前加'@'符号
        local v = clustermc.call(1, "@sdb", "GET", "a")
        logger.info("get a :%s", v)
        local v = clustermc.call(1, "@sdb", "GET", "b")
        logger.info("get b :%s", v)
    end)
end
skynet.start(function()
    local ok, res = xpcall(boot, futil.handle_err)
    if not ok then
        skynet.error(string.format("boot fail::%s", tostring(res)))
    else
        skynet.error("boot success !")
    end
end)
