local skynet = require "skynet"
local socket = require "skynet.socket"
local logger = require "logger"
local skynet_util = require "skynet_util"

local proto_loader = require "proto_loader"
local proto = proto_loader.load("test")
local sproto = require "sproto"

local clustermc = require "clustermc"
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
		--assert(fd == client_fd)	-- You can use fd to reply message
        client_fd = fd
		skynet.ignoreret()	-- session is fd, don't call skynet.ret
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

local command = {}
function command.foobar(req) 
    logger.debug("command.foobar %s", req.what)
    return {ok = true}
end

local function send_client(fd, msg)  
    return clustermc.call("connector1", ".connector_agent", "send_client", fd, msg)
end

function command.client_msg(fd, msg, sz)
    local ok, _type, name, args, response = pcall(host.dispatch, host, msg, sz)
    logger.debug("client_msg: %s", name)
    local f = command[name]
    if f then
        local r = f(args)
        if response then
            send_client(fd, response(r))
        end
    end
end

skynet.start(function()
	skynet.dispatch("lua", function(session,address, cmd, ...)
		return skynet_util.lua_docmd(command, session, string.lower(cmd), ...)
	end)
    skynet.register(".room")
    clustermc.register(".room")
end)
