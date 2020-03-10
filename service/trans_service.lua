local skynet = require "skynet"
require "skynet.manager"
require "tostring"
local logger = require "logger" 
local mysql = require "skynet.db.mysql"
local CMD = {}
local mysql_conf = skynet.getenv("mysql_conf")
local json = require "cjson"
local futil = require "futil"

local mysql_config = json.decode(mysql_conf)
local mysql_db = nil

function CMD.foo(session, param)
    logger.debug("trans_service.foo") 
    return param
end

function CMD.exec_sql(session, sql)
    if not mysql_db then
        logger.fatal("trans_service mysql_db nil")
        return nil
    end
    
    local ok,res = xpcall(mysql_db.query, futil.handle_err,mysql_db, sql)
    if not ok then
        logger.err("db query fail:%s", sql)
        return nil
    end
    return res
end

local function on_connect()
    local ver = mysql_db:query("select version();")
    logger.info("mysql ver:%s", table.tostring(ver))
end

skynet.init(function()
    local conf = mysql_config
    if conf then
        mysql_db = mysql.connect(conf)
        if not mysql_db then
            logger.fatal("trans_service connect mysql fail:%s", table.tostring(conf))
        end
        on_connect()
    end
    skynet.register(".trans_service")
end)

skynet.start(function()
    skynet.dispatch("lua", function(session, source, command, ...)
        local cmd = string.lower(command)
        local f = CMD[cmd]
        if not f then
            return error(string.format("%s Unknown command %s", SERVICE_NAME, tostring(cmd)))
        end
        local ok, err = xpcall(f, handle_error, session, ...)
        if not ok then
            error(err)
        else
            skynet.ret(skynet.pack(err))
        end
    end)
end)
