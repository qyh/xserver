local skynet = require "skynet"
local socketchannel	= require "skynet.socketchannel"
local crypt = require "skynet.crypt"
local socket = require "skynet.socket"
local codec = require "codec"
local logger = require "logger"
local futil = require "futil"
local json = require "cjson"
local node_type = require "node_type"
local protofile = skynet.getenv("proto") or "x"
local proto_loader = require "proto_loader"
local proto = proto_loader.load(protofile)
local sproto = require "sproto"
local host = sproto.new(proto.s2c):host "package"
local request = host:attach(sproto.new(proto.c2s))
local secret = ""
local CMD = {}

local sender = {
    session = 0 
}

local REQ = {}

function REQ.heartbeat(data)
    logger.warn("recv heartbeat :%s", json.encode(data))
end
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
        if t == 'RESPONSE' then
            return session, true, resp 
        else
            local f = REQ[session]
            if f then
                f(resp)
                return 0, true, nil
            end
        end
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

local function auth()
    local clientkey = tostring(math.random()):sub(-8, -1)
    local auth_ok = false
    logger.info('handshake...')
    local handshake = CMD.request(node_type.connector, 'handshake',{
        clientkey = clientkey
    })
    logger.info('handshake res:%s', json.encode(handshake))
    if handshake and handshake.code and handshake.serverkey then
        logger.info('auth...')
        local authres = CMD.request(node_type.connector, 'auth', {
            auth_code = "1234"
        })
        logger.info('auth res:%s', json.encode(authres))
        if authres and authres.ok then
            auth_ok = true
        end
    end
    if auth_ok then
        logger.debug('client request login...')
        local ok, msg = pcall(CMD.request,node_type.login, 'login', {account= "jake", token="1234"})
        logger.debug('request login end:%s', futil.toStr(msg))
    end
    return auth_ok 
end

local function send_test()
    logger.debug('client request...')
    local msg = CMD.request(node_type.room, 'foobar', {what = "hello", value="world"})
    logger.debug('request end:%s', futil.toStr(msg))
    logger.debug('client request3...')
    local msg = CMD.request(node_type.room, 'foobar', {what = "hello", value="world"})
    logger.warn('request3 end:%s, ALL DONE', futil.toStr(msg))
    skynet.timeout(100, send_test)
end

skynet.init(function()
    skynet.timeout(100, function()
        if auth() then
            send_test()
        else
            logger.err("auth failed")
        end
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

