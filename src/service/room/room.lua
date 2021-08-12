local skynet = require "skynet"
local socket = require "skynet.socket"
local logger = require "logger"
local json = require "cjson"
local skynet_util = require "skynet_util"
local handler = require "handler"

local command = {}

function command.foobar(uid, data)
    logger.debug("on room command.foobar:%s %s", uid, json.encode(data))
    skynet.fork(function()
        handler.request_user(uid, "heartbeat", {time = os.time()})
    end)
    return {ok = true}
end

local addr = handler.start(".room", command)

