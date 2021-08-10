local skynet = require "skynet"
local socket = require "skynet.socket"
local clustermc = require "clustermc"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local netpack = require "skynet.netpack"

local CMD = {}
local SOCKET = {}
local gate

local function send_package(fd, pack)
    local package = string.pack(">s2", pack)
    return socket.write(fd, package)
end

function SOCKET.open(fd, addr)
    skynet.call(gate, "lua", "forward", fd, addr)     
end

local function close_agent(fd)
    skynet.call(gate, "lua", "kick", fd)
end

function SOCKET.close(fd)
    print("socket close",fd)
    close_agent(fd)
end

function SOCKET.error(fd, msg)
    print("socket error",fd, msg)
    close_agent(fd)
end

function SOCKET.warning(fd, size)
    -- size K bytes havn't send out in fd
    print("socket warning", fd, size)
end

function SOCKET.data(fd, msg)
end

function CMD.start(conf)
    logger.info("CMD.start:%s", json.encode(conf))
    skynet.call(gate, "lua", "open" , conf)
end

function CMD.close(fd)
    close_agent(fd)
end

function CMD.send_client(fd, msg) 
    logger.info("send_client:%s,%s", fd, msg)
    if fd > 0 then
        return send_package(fd, msg)
    end
    return nil
end

local function dispatch_msg(fd, msg, sz) 
    local nodetype = 2
    local uid = 1
    local str = skynet.tostring(msg, sz)
    --logger.info("dispatch_msg:%s,%s", fd, str)
    clustermc.call(nodetype, "@dispatcher", "request", fd, uid, str, sz)     
    skynet.trash(msg, sz)
end

skynet.register_protocol {
    name = "client",
    id = skynet.PTYPE_CLIENT,
    unpack = function(msg, sz)
        return msg, sz 
    end,
    dispatch = function(fd, _, msg, sz)
        skynet.ignoreret()
        skynet.trace()
        local ok, err = xpcall(dispatch_msg, futil.handle_err, fd, msg, sz)
        if not ok then
            logger.err("dispatch_msg failed fd:%s:%s", fd, err)
        end
    end
}

skynet.start(function()
    skynet.dispatch("lua", function(session, source, cmd, subcmd, ...)
        if cmd == "socket" then
            local f = SOCKET[subcmd]
            if session > 0 then
                skynet.ret(skynet.pack(f(...)))
            else
                f(...)
            end
        else
            local f = assert(CMD[cmd])
            local r = f(subcmd, ...)
            if session > 0 then
                skynet.ret(skynet.pack(r))
            end
        end
    end)

    gate = skynet.newservice("connector")
    clustermc.register("xwatchdog", skynet.self())
end)
