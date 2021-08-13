local skynet = require "skynet"
local logger = require "logger"
local clustermc = require "clustermc"
local json = require "cjson"
local protofile = skynet.getenv("proto") or "x"
local proto_loader = require "proto_loader"
local proto = proto_loader.load(protofile)
local sproto = require "sproto"
local node_type = require "node_type"
local netpack = require "skynet.netpack"
require "tostring"
require "skynet.manager"    -- import skynet.register
local host = sproto.new(proto.c2s):host "package"
local request = host:attach(sproto.new(proto.s2c))
local db = {}
local my_type = tonumber(skynet.getenv("nodetype"))

local command = {}
local handler = {}
local uid_data = {}
local fd_data = {}

local function send_client(userdata, msg)
    if not (userdata and userdata.fd and userdata.connector) then
        return false, "userdata nil"
    end
    local ok, err = clustermc.call_node(userdata.connector, "@xwatchdog", "send_client", my_type, userdata.fd, msg)
end

function command.register(dst, cmds)
    logger.debug("register %s %s", dst, json.encode(cmds))
    if not (cmds and next(cmds)) then
        return false
    end
    for _, cmd in pairs(cmds) do
        handler[cmd] = dst
    end
    return true
end

local function dispatch_to_handler(cmd, uid, data, response)
    local dst = handler[cmd]
    if dst then
        if response then
            return skynet.call(dst, "lua", cmd, uid, data)
        else
            skynet.send(dst, "lua", cmd, uid, data)
        end
    end
end

--send request to client
function command.request_user(uid, cmd, data)
    local user_data  = uid_data[uid]
    local msg = request(cmd, data, 1)
    return send_client(user_data, msg)
end

function command.get_user_data(fd)
    return fd_data[fd] or uid_data[fd]
end

function command.request(connector, fd, uid, msg, sz)
    logger.debug("command.request %s fd:%s, uid:%s", connector, fd, uid)
    fd_data[fd] = {
        fd = fd,
        uid = uid,
        connector = connector,
    } 
    if uid then
        uid_data[uid] = fd_data[fd]
    end
    local _type, cmd, data, response = host:dispatch(msg, sz)
    logger.debug("command.request cmd:%s, data:%s, %s", cmd, json.encode(data), _type)
    logger.debug("response:%s", response )
    if _type == 'REQUEST' then
        local f = command[cmd]
        if f then
            local r = f(data)
            if response then
                local str = response(r)
                send_client(fd_data[fd], str)
            end
        else
            local r = dispatch_to_handler(cmd, uid or fd, data, response)
            if response and r then
                local res = response(r)
                send_client(fd_data[fd], res)
            end
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
