local skynet = require "skynet"
local socket = require "skynet.socket"
local logger = require "logger"

local proto_loader = require "proto_loader"
local proto = proto_loader.load("test")
local sproto = require "sproto"
require "tostring"
require "skynet.manager"

local WATCHDOG

local host = sproto.new(proto.c2s):host "package"
local send_request = host:attach(sproto.new(proto.s2c))

local CMD = {}
local REQUEST = {}
local client_fd

function REQUEST:foobar(fd, args)
    logger.debug(string.format("foobar %s", self.what))
    return { ok = true }
end

function REQUEST:handshake()
    return { msg = "Welcome to skynet, I will send heartbeat every 5 sec." }
end

function REQUEST:quit()
    skynet.call(WATCHDOG, "lua", "close", client_fd)
end

local function request(name, args, response)
    logger.info("request name:%s", name, response)
    local f = REQUEST[name]
    if f then
        local r = f(args)
        if response then
            return response(r)
        end
    else
        return false
    end
end

local function send_package(pack)
    local package = string.pack(">s2", pack)
    socket.write(client_fd, package)
end

skynet.register_protocol {
    name = "client",
    id = skynet.PTYPE_CLIENT,
    unpack = function (msg, sz)
        logger.info("connector_agent:%s:%s", msg, sz)
        return host:dispatch(msg, sz)
    end,
    dispatch = function (fd, _, type, ...)
        --assert(fd == client_fd)    -- You can use fd to reply message
        client_fd = fd
        skynet.ignoreret()    -- session is fd, don't call skynet.ret
        skynet.trace()
        if type == "REQUEST" then
            local ok, result  = pcall(request, ...)
            if ok then
                if result then
                    send_package(result)
                end
            else
                skynet.error(result)
            end
        else
            assert(type == "RESPONSE")
            error "This example doesn't support request client"
        end
    end
}

function CMD.start(conf)
    
end

function CMD.disconnect()
    -- todo: do something before exit
end

skynet.start(function()
    skynet.dispatch("lua", function(_,_, command, ...)
        skynet.trace()
        local f = CMD[command]
        skynet.ret(skynet.pack(f(...)))
    end)
    skynet.register(".connector_agent")
end)
