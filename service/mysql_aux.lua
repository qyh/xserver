local service = ".mysql_service"
local logger = require "logger"
local skynet = require "skynet"
local dbconf = require "db.db"

local mysql_conf = dbconf.mysql
local mysql_aux = {}


function mysql_aux.exec_sql(dbname, sql)
    if not mysql_aux[dbname] then
        logger.err("no dbname:%s found", dbname)
        return nil
    end
    return skynet.call(service, "lua", "exec_sql", dbname, sql)
end

for dbname, conf in pairs(mysql_conf) do
    mysql_aux[dbname] = {}
    mysql_aux[dbname].exec_sql = function(sql)
        return mysql_aux.exec_sql(dbname, sql)
    end
end

return mysql_aux
