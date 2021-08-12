local skynet = require "skynet"
local socket = require "skynet.socket"
local logger = require "logger"
local skynet_util = require "skynet_util"

local proto_loader = require "proto_loader"
local protofile = skynet.getenv("proto") or "x"
local proto = proto_loader.load(protofile)
local sproto = require "sproto"

require "tostring"
require "skynet.manager"
local handler = {}

local command = {}
function handler.request_user(uid, cmd, data)
    skynet.call(".dispatcher", "lua", "request_user", uid, cmd, data)
end
function handler.start(name, cmds)
    command = cmds
    skynet.start(function()
        skynet.dispatch("lua", function(session,address, cmd, ...)
            return skynet_util.lua_docmd(command, session, string.lower(cmd), ...)
        end)
    end)
    skynet.register(name)
    local cmdnames = {}
    for cmd, fuc in pairs(cmds) do
        if type(fuc) == "function" then
            table.insert(cmdnames, cmd)
        end
    end
    local self = skynet.self()
    skynet.send(".dispatcher", "lua", "register", self, cmdnames)
    return self
end
return handler
