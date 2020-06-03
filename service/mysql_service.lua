local skynet = require "skynet"
require "skynet.manager"
require "tostring"
local logger = require "logger" 
local mysql = require "skynet.db.mysql"
local CMD = {}
local mysql_conf = skynet.getenv("mysql_conf")
local json = require "cjson"
local futil = require "futil"
local db = skynet.getenv("db") or "db"
local dbconf = require ("db."..db)
local agents = {}

function CMD.exec_sql(session, dbname, sql)
    local agent = agents[dbname]
    if not agent then
        logger.err("not dbname:%s found", dbname)
        return nil
    end
    return skynet.call(agent, "lua", "exec_sql", sql)
end

function CMD.init()
    if dbconf and next(dbconf) and dbconf.mysql then
        for k, conf in pairs(dbconf.mysql) do
            local agent = skynet.newservice("mysql_agent")
            if agent then
                local ok, rv = xpcall(skynet.call, futil.handle_err, agent, "lua", "init", conf)
                if ok and rv then
                    agents[k] = agent
                    logger.info("connect db:%s success", k)
                else
                    logger.err("connect db:%s fail ,conf:%s", k, futil.toStr(conf))
                end
            end
        end
    end
end

skynet.init(function()
    skynet.register(".mysql_service")
    CMD.init()
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

