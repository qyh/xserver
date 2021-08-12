local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
local name = skynet.getenv("name")
local mt_lock = require "mt_lock"
local function boot()
    skynet.newservice("logservice")
    skynet.newservice("redis_pubsub")
    skynet.newservice("mt_lock_service")
    local sdb = skynet.newservice("simpledb")
    local t = os.time()
    for i=1, 10 do
        local l = mt_lock.new_lock("testlock", i, 10)
        l.unlock()
    end
    logger.warn("done!")
end
skynet.start(function()
    local ok, res = xpcall(boot, futil.handle_err)
    if not ok then
        skynet.error(string.format("%s boot fail::%s", name, tostring(res)))
    else
        skynet.error(name .. "boot success !")
    end
end)
