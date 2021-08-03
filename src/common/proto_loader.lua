local skynet = require "skynet"
local futil = require "futil"
local sprotoparser = require "sprotoparser"
local logger = require "logger"
local loader = {}

function loader.load(filename) 
    local ok, res = xpcall(function()
        local c2spath = "../protos/"..filename..".c2s"
        local c2s_f = io.open(c2spath, "r")
        if not c2s_f then
            logger.err("%s not exists", c2spath)
        end
        local c2s_content = c2s_f:read("*a")
        c2s_f:close()

        local s2cpath = "../protos/"..filename..".s2c"
        local s2c_f = io.open(s2cpath, "r")
        if not s2c_f then
            logger.err("%s not exists", s2cpath)
        end
        local s2c_content = s2c_f:read("*a")
        s2c_f:close()
        return {c2s = sprotoparser.parse(c2s_content), s2c = sprotoparser.parse(s2c_content)}
    end, futil.handle_err)
    if ok then
        return res
    else
        logger.err(res)
    end
    return {} 
end

return loader
