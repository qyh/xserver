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
function mysql_aux.get_insert_sql(tablename, data)
    local sql = ""
    local fields = ""
    local values = ""
    for k, v in pairs(data) do
        k = string.format("`%s`",k)
        fields = #fields > 0 and fields..","..k or fields..k 
        if type(v) == "string" then
            values = #values > 0 and values..","..mqv(v) or values..mqv(v)
        else
            values = #values > 0 and values..","..v or values..v
        end
    end
    sql = string.format("insert into %s (%s) values(%s);", tablename, fields, values)
    return sql
end
function mysql_aux.get_delete_sql(tablename, keys)
    local wstr = ""
    local sql = ""
    for k, v in pairs(keys) do
        k = string.format("`%s`",k)
        if type(v) == "string" then
            wstr = #wstr > 0 and wstr.." and "..k.."="..mqv(v) or 
                wstr..k.."="..mqv(v)
        else
            wstr = #wstr > 0 and wstr.." and "..k.."="..v or 
                wstr..k.."="..v 
        end
    end
    sql = string.format([[delete from %s where %s;]], tablename, wstr)
    return sql
end
function mysql_aux.get_query_sql(tablename, keys, fields, ext)
    local fstr = ""
    local wstr = ""
    local sql = ""
    if (fields and type(fields) == 'table' and next(fields)) then
        for k, v in pairs(fields) do
            k = string.format("`%s`",k)
            fstr = #fstr > 0 and fstr..","..k or fstr..k
        end
    else
        fstr = "*"
    end
    for k, v in pairs(keys) do
        k = string.format("`%s`",k)
        if type(v) == "string" then
            wstr = #wstr > 0 and wstr.." and "..k.."="..mqv(v) or 
                wstr..k.."="..mqv(v)
        else
            wstr = #wstr > 0 and wstr.." and "..k.."="..v or 
                wstr..k.."="..v 
        end
    end
    if not ext then
        ext = ""
    end
    sql = string.format([[select %s from %s where %s %s]], fstr, tablename, wstr, ext)
    return sql
end
function mysql_aux.get_update_sql(tablename, keys, data)
    local sql = ""
    local set_str = ""
    local where_str = ""
    for k, v in pairs(data) do
        k = string.format("`%s`",k)
        if type(v) == "string" then
            set_str = #set_str > 0 and set_str..","..k.."="..mqv(v) or set_str..k.."="..mqv(v)  
        else
            set_str = #set_str > 0 and set_str..","..k.."="..v or set_str..k.."="..v  
        end
    end
    for k, v in pairs(keys) do
        k = string.format("`%s`",k)
        if type(v) == "string" then
            where_str = #where_str > 0 and where_str.." and ".. k.."="..mqv(v) or where_str .. k.."="..mqv(v)
        else
            where_str = #where_str > 0 and where_str.." and ".. k.."="..v or where_str .. k.."="..v 
        end
    end
    sql = string.format([[update %s set %s where %s;]], tablename, set_str, where_str)
    return sql
end
return mysql_aux
