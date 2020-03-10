local skynet = require "skynet"
require "skynet.manager"
require "tostring"
local logger = require "logger" 
local mysql = require "skynet.db.mysql"
local CMD = {}
local json = require "cjson"
local futil = require "futil"

local mysql_config = nil 
local mysql_db = nil
local wait_cos = {}

function CMD.foo(session, param)
    logger.debug("trans_service.foo") 
    return param
end

local function connect()
    local conf = mysql_config
    if conf then
        mysql_db = mysql.connect(conf)
        if not mysql_db then
            logger.fatal("mysql_agent connect mysql fail:%s", table.tostring(conf))
            return false
        end
    end
    return true
end

function CMD.exec_sql(session, sql)
    if not mysql_db then
        logger.fatal("trans_service mysql_db nil")
        return nil
    end
    while true do
        local ok,rv= xpcall(mysql_db.query, futil.handle_err,mysql_db, sql)
        if not ok then
            logger.err("db query fail:%s", sql)
            local co = coroutine.running()
            table.insert(wait_cos, co)
            skynet.wait(co)
        else
            res = rv
            break
        end
    end
    return res
end

function CMD.init(session, conf)
    mysql_config = conf
    return connect()
end
local function check_co()
    while true do
        if wait_cos and next(wait_cos) then
            while #wait_cos > 0 do
                local co = table.remove(wait_cos)
                skynet.wakeup(co)
            end
        end
        skynet.sleep(10)
    end
end
skynet.init(function()
    skynet.fork(check_co) 
end)

skynet.start(function()
    skynet.dispatch("lua", function(session, source, command, ...)
        local cmd = string.lower(command)
        local f = CMD[cmd]
        if not f then
            return error(string.format("%s Unknown command %s", SERVICE_NAME, tostring(cmd)))
        end
        local ok, err = xpcall(f, futil.handle_err, session, ...)
        if not ok then
            error(err)
        else
            skynet.ret(skynet.pack(err))
        end
    end)
end)
