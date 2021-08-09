local skynet = require "skynet"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local clustermc = require "clustermc"
local function boot()
    skynet.newservice("logservice")
    local sdb = skynet.newservice("simpledb")
    skynet.newservice("clustermgr") 
	print(skynet.call(sdb, "lua", "SET", "a", "cluster3a"))
	print(skynet.call(sdb, "lua", "SET", "b", "cluster3b"))
	print(skynet.call(sdb, "lua", "GET", "a"))
	print(skynet.call(sdb, "lua", "GET", "b"))
    clustermc.register("sdb", sdb)
end
skynet.start(function()
    local ok, res = xpcall(boot, futil.handle_err)
    if not ok then
        skynet.error(string.format("boot fail::%s", tostring(res)))
    else
        skynet.error("boot success !")
    end
end)
