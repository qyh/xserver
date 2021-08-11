local skynet = require "skynet"
local socketchannel	= require "skynet.socketchannel"
local crypt = require "skynet.crypt"
local socket = require "skynet.socket"
local codec = require "codec"
local logger = require "logger"
local futil = require "futil"
local node_type = require "node_type"

local proto_loader = require "proto_loader"
local proto = proto_loader.load("test")
local sproto = require "sproto"
local host = sproto.new(proto.s2c):host "package"
local request = host:attach(sproto.new(proto.c2s))

local CMD = {}

local sender = {
    session = 0 
}
local function dispatch64(so)
	local text = so:read(2)
    if not text then
        logger.debug('dispatch read text: fail')
        return nil, false, nil
    end
    local s = text:byte(1) * 256 + text:byte(2)
    local msgbase64 = so:read(s)
    local msg = codec.base64_decode(msgbase64)
    local t, session, resp = host:dispatch(msg)
	return session, true, resp 
end
local function dispatch(so)
	local text = so:read(2)
    if not text then
        logger.debug('dispatch read text: fail')
        return nil, false, nil
    end
    local s = text:byte(1) * 256 + text:byte(2)
    local msg = so:read(s)
    local node_type = msg:byte(1)
    local err_code = (msg:byte(2) << 8) + msg:byte(3)
    local protoMsg = msg:sub(4)
    logger.debug("recv node_type:%s, err_code:%s", node_type, err_code)
    if err_code == 0 then
        local t, session, resp = host:dispatch(protoMsg)
        return session, true, resp 
    else
        return session, false, nil  
    end
end
function CMD.request(node_type, name, data)
    sender.session = sender.session + 1
    local str = request(name, data, sender.session)
    local m = string.char(node_type)
    m = m..str
    local msg = string.pack(">s2", m)
    return sender.__sock:request(msg, sender.session)
end

function CMD.base64_request(name, data)
    sender.session = sender.session + 1
    local str = request(node_type.room, name, data, sender.session)
    local msg = string.pack(">s2", codec.base64_encode(str))
    return sender.__sock:request(msg, sender.session)
end

function CMD.connect(info)
    sender.__sock = socketchannel.channel {
        host = info.host,
        port = info.port,
        response = dispatch,
        nodelay = true
    }
    return sender.__sock:connect(true)
end

skynet.init(function()
    skynet.timeout(100, function()
        logger.debug('client request...')
        local msg = CMD.request(node_type.room, 'foobar', {what = "hello", value="world"})
        logger.debug('request end:%s', futil.toStr(msg))
        logger.debug('client request2...')
        local ok, msg = pcall(CMD.request,node_type.login, 'foobar', {what = "hello", value="world"})
        logger.debug('request2 end:%s', futil.toStr(msg))
        logger.debug('client request3...')
        local msg = CMD.request(node_type.room, 'foobar', {what = "hello", value="world"})
        logger.warn('request3 end:%s, ALL DONE', futil.toStr(msg))
    end)
end)

skynet.start(function()
	skynet.dispatch('lua', function(session, address, cmd, ...)
		local f = CMD[cmd]
		if f then
			if session > 0 then
				skynet.ret(skynet.pack(f(...)))
			else
				f(...)
			end
		else
			logger.err('ERROR: Unknown command:%s', tostring(cmd))
		end
	end)
end)

