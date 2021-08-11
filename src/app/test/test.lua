local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
local name = skynet.getenv("name")
local function boot()
    skynet.newservice("logservice")
    local sdb = skynet.newservice("simpledb")
    local t = os.time()
    while os.time() - t < 30 do
        skynet.call(sdb, "lua", "SET", "a", "I am a, hello")
        local a = skynet.call(sdb, "lua","GET", "a")
        logger.debug("a:%s", a)
        skynet.sleep(10)
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
