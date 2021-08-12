local skynet = require "skynet"
local socket = require "skynet.socket"
local clustermc = require "clustermc"
local logger = require "logger"
local json = require "cjson"
local futil = require "futil"
local netpack = require "skynet.netpack"
local proto_loader = require "proto_loader"
local protofile = skynet.getenv("proto")
local proto = proto_loader.load(protofile) or "x"
local sproto = require "sproto"
local host = sproto.new(proto.c2s):host "package"
local error_code = require "error_code"

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

--msg 为proto消息
local function pack_response(node_type, err_code, msg) 
    --打包回应消息
    --第1个字节为结点类型
    --第2~3个字结为错误码
    --后面跟proto消息
    msg = msg or ""
    err_code = err_code & 0xFFFF
    local str = string.char(node_type) 
    str = str..string.char(err_code >> 8)..string.char(err_code & 0x00FF)
    return str..msg
end

function CMD.send_client(node_type, fd, msg) 
    logger.info("send_client:%s,%s, nodetype:%s", fd, msg, node_type)
    if fd > 0 then
        local m = pack_response(node_type, error_code.OK, msg)
        return send_package(fd, m)
    end
    return nil
end

local function dispatch_msg(fd, msg, sz) 
    local uid = 1
    local str = skynet.tostring(msg, sz)
    --logger.info("dispatch_msg:%s,%s", fd, str)
    --解析消息头
    --第一个字节为结点类型(node_type.lua)
    local nodetype = str:byte(1)
    local protoMsg = str:sub(2)
    local ok,err = clustermc.call(nodetype, "@dispatcher", "request", fd, uid, protoMsg, #protoMsg)     
    if not ok then
        local _, cmd, data, response = host:dispatch(protoMsg, #protoMsg)
        logger.err("call %s failed, data:%s", cmd, json.encode(data))
        if response then
            local res_msg = pack_response(nodetype, error_code.NODE_NOT_FOUND, ret)
            send_package(fd, res_msg)

        end
    else
        logger.info("clustermc call node:%s %s %s", nodetype, ok, err)
    end
    --skynet.trash(msg, sz)
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
