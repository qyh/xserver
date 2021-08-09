local skynet = require "skynet"
local gateserver = require "snax.gateserver"
local logger = require "logger"
local codec = require "codec"
local netpack = require "skynet.netpack"

local connection = {}    -- fd -> connection : { fd , client, agent , ip, mode }
local forwarding = {}    -- agent -> connection
local WATCHDOG
skynet.register_protocol {
    name = "client",
    id = skynet.PTYPE_CLIENT,
}

local handler = {}

function handler.open(source, conf)
    logger.info("handler.open:%s", source)
    WATCHDOG = source
    return true
end

function handler.message(fd, msg, sz)
    -- recv a package, forward it
    local c = connection[fd]
    logger.debug("connector handler.messag")
    --[[
    local b64m = netpack.tostring(msg, sz) 
    local m = codec.base64_decode(b64m) 
    logger.debug("connector handler.messag %s", m)
    ]]
    skynet.redirect(WATCHDOG, 0, "client", fd, msg, sz)
    --skynet.trash(msg,sz)
    --[[
    local agent = c.agent
    if agent then
        -- It's safe to redirect msg directly , gateserver framework will not free msg.
        skynet.redirect(agent, c.client, "client", fd, msg, sz)
    else
        --skynet.send(watchdog, "lua", "socket", "data", fd, skynet.tostring(msg, sz))
        -- skynet.tostring will copy msg to a string, so we must free msg here.
        skynet.trash(msg,sz)
    end
    ]]
end

function handler.connect(fd, addr)
    local c = {
        fd = fd,
        ip = addr,
    }
    connection[fd] = c
    skynet.call(WATCHDOG, "lua", "socket", "open", fd, addr)
end

local function unforward(c)
end

local function close_fd(fd)
    local c = connection[fd]
    if c then
        unforward(c)
        connection[fd] = nil
    end
end

function handler.disconnect(fd)
    close_fd(fd)
end

function handler.error(fd, msg)
    close_fd(fd)
end

function handler.warning(fd, size)
end

local CMD = {}

function CMD.forward(source, fd, address)
    gateserver.openclient(fd)
end

function CMD.accept(source, fd)
    local c = assert(connection[fd])
    unforward(c)
    gateserver.openclient(fd)
end

function CMD.kick(source, fd)
    gateserver.closeclient(fd)
end

function handler.command(cmd, source, ...)
    local f = assert(CMD[cmd])
    return f(source, ...)
end

gateserver.start(handler)
