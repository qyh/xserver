local skynet = require "skynet"
local logger = require "logger"
local clustermc = require "clustermc"
local json = require "cjson"
local proto_loader = require "proto_loader"
local proto = proto_loader.load("test")
local sproto = require "sproto"
local node_type = require "node_type"
local netpack = require "skynet.netpack"
require "tostring"
require "skynet.manager"    -- import skynet.register
local host = sproto.new(proto.c2s):host "package"
local db = {}
local my_type = node_type.room

local command = {}

local function send_client(fd, msg)
    local ok, err = clustermc.call(node_type.connector, "@xwatchdog", "send_client", my_type, fd, msg)
end

function command.get(key)
    return db[key]
end

function command.set(key, value)
    logger.debug("SET %s %s", key, value)
    local last = db[key]
    db[key] = value
    return last
end

function command.foobar(data)
    logger.info("command.foobar:%s", json.encode(data))
    return {ok = true}
end

function command.request(fd, uid, msg, sz)
    logger.debug("command.request fd:%s, uid:%s", fd, uid)
    local _type, cmd, data, response = host:dispatch(msg, sz)
    logger.debug("command.request cmd:%s, data:%s, %s", cmd, json.encode(data), _type)
    logger.debug("response:%s", response )
    if _type == 'REQUEST' then
        local f = command[cmd]
        if f then
            local r = f(data)
            if response then
                local str = response(r)
                send_client(fd, str)
            end
        else
            logger.err("no %s fuction", cmd)
        end
    end
    return "OK" 
end

skynet.start(function()
    skynet.dispatch("lua", function(session, address, cmd, ...)
        local f = command[cmd]
        if f then
            if session > 0 then
                skynet.ret(skynet.pack(f(...)))
            else
                f(...)
            end
        else
            logger.err(string.format("Unknown command %s", tostring(cmd)))
        end
    end)
    skynet.register ".dispatcher"
    clustermc.register("dispatcher", skynet.self())
end)
