local skynet = require "skynet"
require "skynet.manager"
require "tostring"
local logger = require "logger" 
local CMD = {}
local json = require "cjson"
local futil = require "futil"
local dbconf = require "db.db"
local mysql_conf = dbconf.mysql
local mysql_aux = require "mysql_aux"
local redis = require "pubsub"



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
                    local rv = mysql_aux[db].exec_sql(sql) 
                    logger.debug("request result:%s %s rv:%s", db, i, table.tostring(rv))
                    skynet.sleep(200)
                end
            end, db)
        end
        break
    end
end
local function get_last_id()
    local rkey = "lastID"
    local lastID = redis:get(rkey) or 0
    return lastID
end

local function update_last_id(id)
    local rkey = "lastID"
    return redis:set(rkey, id)
end

local function save_rank_to_file()
    logger.debug("save_rank_to_file")
    local rdsKey = "RechargeRank"
    local rds = redis:zrevrange(rdsKey, 0, 10000, 'withscores')
    if rds and next(rds) then
        logger.debug("opening file...")
        local of = io.open("recharge_rank.lua", "w")
        logger.debug("opening file end")
        logger.debug('redis get result success, rows:%s', #rds/2)
        local text = "local rank = { \n"
        for i=1, #rds,2 do 
            text = text..string.format("[%s] = %s,\n", rds[i], rds[i+1]) 
        end
        text = text.."}\nreturn rank"
        of:write(text)
        of:flush()
        of:close()
        logger.debug("save rank to file success")
    else
        logger.err('redis get result fail')
    end
end

local function calc_rank()
    local lastID = get_last_id()
    local order_tables = {"OnlinePayNotify2017", "OnlinePayNotify2018", "OnlinePayNotify2019"}
    --local order_tables = {"OnlinePayNotify_tmptest"}
    local begin_t = os.time()
    local rdsKey = "RechargeRank"
    for k, tbname in pairs(order_tables) do
        logger.debug('deal with table:%s', tbname)
        while true do
            local _t = os.time()
            logger.debug('query from ID:%s', lastID)
            local sql = string.format("select * from %s where ID > %s order by ID asc limit 10000",
            tbname, lastID) 
            local res = mysql_aux.localhost.exec_sql(sql)
            if not (res and next(res)) then
                break
            else
                for k, v in pairs(res) do
                    redis:zincrby(rdsKey, v.totalFee, v.userID)
                    update_last_id(v.ID)
                end
                lastID = res[#res].ID
                logger.debug("update lastID to:%s", lastID)
                update_last_id(res[#res].ID)
            end
            logger.debug('deal with 10000 row take time:%s', os.time() - _t)
            skynet.sleep(100)
        end
        logger.debug('deal with table:%s end', tbname)
    end
    local end_t = os.time()
    logger.debug("take time:%s", end_t - begin_t)
    begin_t = end_t
    save_rank_to_file() 
    logger.debug("save to file take time:%s", os.time() - begin_t)
end

local function is_table_exists(conn, dbname, tname)
    local sql = string.format([[SELECT DISTINCT t.table_name, n.SCHEMA_NAME FROM information_schema.TABLES t, 
        information_schema.SCHEMATA n WHERE t.table_name = '%s' AND n.SCHEMA_NAME = '%s';]], tname, dbname)

    local rv = mysql_aux[conn].exec_sql(sql)
    if not (rv and next(rv)) then
        return false
    else
        return true
    end
end

local function calc_active_day(userID)
    --local rechargeRank = require "recharge_rank"
    local begin_time = futil.getTimeByDate("2017-01-01 00:00:00")
    local end_time = futil.getTimeByDate("2020-01-01 00:00:00")
    local prefix = "UserLog_"
    local val_time = begin_time
    while true do
        local tname = prefix..futil.monthStr(val_time, "_")
        local db = nil
        for k, conf in pairs(mysql_conf) do
            local dbname = conf.database
            if dbname == 'Zipai' then
                if is_table_exists(k, dbname, tname) then
                    logger.debug('table:%s exists in %s', dbname.."."..tname, k)
                    db = k       
                    break
                end
            end
        end
        if db then
            logger.debug("do query in :%s,%s", db, tname)
        else
            logger.err("table not exists:%s", tname)
        end
        val_time = futil.get_next_month(val_time)
        if val_time >= end_time then
            break
        end
        skynet.sleep(100)
    end
    logger.debug("calc_active_day done !")
end

local function export_data()
    calc_active_day()
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

