local skynet = require "skynet"
require "skynet.manager"
require "tostring"
local logger = require "logger" 
local CMD = {}
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
local mysql_conf = dbconf.mysql
function CMD.init()
    
end
local function test()
    while true do
        local db = nil
        for dbname, conf in pairs(mysql_conf) do
            --[[
            local sql = "select version();"
            local rv = skynet.call(".mysql_service", "lua", "exec_sql", dbname, sql)
            logger.debug("export_data rv:%s", table.tostring(rv))
            ]]
            db = dbname
            skynet.fork(function(db)
                for i=1, 100 do 
                    logger.debug("request:%s, db:%s", db, i)
                    local sql = "select version();"
                    local rv = skynet.call(".mysql_service", "lua", "exec_sql", db, sql)
                    logger.debug("request result:%s %s rv:%s", db, i, table.tostring(rv))
                    skynet.sleep(200)
                end
            end, db)
        end
        break
    end
end
local function export_data()
end

skynet.init(function()
    CMD.init()
    skynet.timeout(200, export_data)
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

