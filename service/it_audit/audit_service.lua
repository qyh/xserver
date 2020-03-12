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
local user = require "user"

local audit = {}

function CMD.init()
    
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

function audit.audit_active_day()
    logger.debug("audit.audit_active_day")
end

function audit.audit_game_win_lose()
    logger.debug("audit.audit_game_win_lose")
end

function audit.audit_recharge()
    logger.debug("audit.audit_recharge")
    local rank_user = require "recharge_rank"
    local order_tables = {"OnlinePayNotify2017", "OnlinePayNotify2018", "OnlinePayNotify2019"}
    --local order_tables = {"OnlinePayNotify_tmptest"}
    local sql = "select * from Mall"
    local mall_info = {}
    local rv = mysql_aux["2019_118"].exec_sql(sql)
    if not (rv and next(rv)) then
        logger.err("get mall info fail")
        return
    end
    for k, v in pairs(rv) do
        mall_info[v.ID] = v
    end
    logger.debug("get mall info success")
    local lastID = 0
    local begin_t = os.time()
    for k, tbname in pairs(order_tables) do
        logger.debug('audit_recharge deal with table:%s', tbname)
        while true do
            local _t = os.time()
            logger.debug('audit_recharge query from ID:%s,table:%s', lastID, tbname)
            local sql = string.format("select * from %s where ID > %s order by ID asc limit 10000",
            tbname, lastID) 
            local res = mysql_aux.localhost.exec_sql(sql)
            if not (res and next(res)) then
                break
            else
                for k, v in pairs(res) do
                    if rank_user[v.userID] then
                        local mall_detail = mall_info[v.mallID]
                        local user_info = user.get_user_info(v.userID) or {}
                        user_info.gameCardRecharge = user_info.gameCardRecharge or 0 
                        user_info.rechargeCount = user_info.rechargeCount or 0
                        user_info.goldCoinRecharge = user_info.goldCoinRecharge or 0
                        user_info.rechargeCount = user_info.rechargeCount + 1
                        if mall_detail then
                            local gain_goods = mall_detail.gainGoods
                            local arr = futil.split(gain_goods, "=")
                            if #arr == 2 then
                                local goodsID = tonumber(arr[1])
                                local goodsCount = tonumber(arr[2])
                                if goodsID == 107 then
                                    user_info.gameCardRecharge = user_info.gameCardRecharge + (goodsCount * v.amount)
                                elseif goodsID == 0 then
                                    --logger.debug("add goldCoin userID:%s:%s", v.userID, goodsCount)
                                    user_info.goldCoinRecharge = user_info.goldCoinRecharge + (goodsCount * v.amount)
                                end
                                user.set_user_info(v.userID, user_info)
                            end
                        end
                    end
                end
                lastID = res[#res].ID
                logger.debug("update lastID to:%s", lastID)
            end
            logger.debug('audit_recharge deal with 10000 row take time:%s sec', os.time() - _t)
            skynet.sleep(100)
        end
        logger.debug('deal with table:%s end', tbname)
    end
    local end_t = os.time()
    logger.debug("audit_recharge done !! take time:%s sec", end_t - begin_t)
end

local function run()
    local audit_job = skynet.getenv("audit_job")
    if not audit_job then
        logger.err("no job specified, will do nothing")
        return 
    end
    if audit[audit_job] then
        local f = audit[audit_job]
        local ok, err = xpcall(f, futil.handle_err)
        if not ok then
            logger.err("exec job fail:%s:%s", audit_job, tostring(err))
        end
    else
        logger.err("no nob handle found:%s", audit_job)
    end
end


skynet.init(function()
    CMD.init()
    skynet.timeout(200, run)
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

