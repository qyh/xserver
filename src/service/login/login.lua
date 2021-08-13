local skynet = require "skynet"
local socket = require "skynet.socket"
local logger = require "logger"
local crypt = require "skynet.crypt"
local json = require "cjson"
local skynet_util = require "skynet_util"
local handler = require "handler"
local clustermc = require "clustermc"

local command = {}
local auth_data = {}
local function send_user(uid, cmd, data)
    return handler.request_user(uid, cmd, data)
end

function command.login(fd, data) 
    logger.debug("login:%s %s", fd, json.encode(data))
    local uid = 1
    local user_data = {
        uid = uid 
    }
    local user_conn_info = handler.get_user_data(fd)
    if user_conn_info then
        local ok = clustermc.call_node(user_conn_info.connector, "@xwatchdog","login_ok", fd, uid)
    end
    return {ok = true, user_data = user_data} 
end

local addr = handler.start(".login", command)

