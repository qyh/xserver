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
local redis_aux = require "redis_aux"
local user = require "user"
local const = require "const"

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

local function clear_audit_user()
    local rank_user = require "recharge_rank"
    for userID, amount in pairs(rank_user) do
        local rkey = string.format("%s:%s",const.redis_key.audit_user, userID)
        redis:del(rkey)
    end
end

local function get_first_recharge_time(userID)
    local order_tables = {"OnlinePayNotify2017", "OnlinePayNotify2018", "OnlinePayNotify2019"}
    for k, tbname in pairs(order_tables) do
        local sql = string.format("select * from %s where userID=%s order by ID asc limit 1",
        tbname, userID)
        local rv = mysql_aux["localhost"].exec_sql(sql)
        if rv and next(rv) then
            local order = rv[1]
            return order.notifyTime
        end
    end
    return ""
end

local function get_last_recharge_time(userID)
    local order_tables = {"OnlinePayNotify2019", "OnlinePayNotify2018", "OnlinePayNotify2017"}
    for k, tbname in pairs(order_tables) do
        local sql = string.format("select * from %s where userID=%s order by ID desc limit 1",
        tbname, userID)
        local rv = mysql_aux["localhost"].exec_sql(sql)
        if rv and next(rv) then
            local order = rv[1]
            return order.notifyTime
        end
    end
    return ""
end

function audit.audit_base_info()
    logger.debug("writing user base info")
    local rank_user = require "recharge_rank"
    for userID, amount in pairs(rank_user) do
        local rkey = string.format("%s:%s",const.redis_key.audit_user, userID)
        local sql = string.format("select * from UserData where userID=%s", userID)
        local rv = mysql_aux["2019_118"].exec_sql(sql)
        if not (rv and next(rv)) then
            logger.err("get userData info fail:%s", userID)
        else
            local info = rv[1]
            local user_info = {}
            user_info.regIp = info.regIp 
            user.set_user_info(userID, user_info)
            logger.debug("write userData.ip done:%s,%s", userID, user_info.regIp)
        end
        skynet.sleep(10)
    end
    logger.info("audit_base_info set userData.regIp ALL DONE !!")
    --[[
    for userID, amount in pairs(rank_user) do
        local rkey = string.format("%s:%s",const.redis_key.audit_user, userID)
        local sql = string.format("select * from User where ID=%s", userID)
        local rv = mysql_aux["2019_118"].exec_sql(sql)
        if not (rv and next(rv)) then
            logger.err("get user base info fail:%s", userID)
        else
            sql = string.format("select * from Account where userID=%s", userID)
            local acc_info = mysql_aux["2019_118"].exec_sql(sql)
            if not (acc_info and next(acc_info)) then
                logger.err("get user account info fail:%s", userID)
                acc_info = {goldCoin = 0}
            else
                acc_info = acc_info[1]
            end
            local info = rv[1]
            local user_info = {}
            user_info.code = info.code
            user_info.userID = userID
            user_info.nickName = info.nickName
            user_info.userName = info.userName
            user_info.regTime = info.regTime
            user_info.goldCoin = acc_info.goldCoin
            user_info.level = info.level
            user_info.totalAmount = amount
            user_info.lastLoginTime = info.recentLoginTime
            user_info.firstRechargeTime = get_first_recharge_time(userID)
            user_info.lastRechargeTime = get_last_recharge_time(userID)
            user.set_user_info(userID, user_info)
            logger.debug("write user base info done:%s", userID)
        end
        skynet.sleep(10)
    end
    logger.info("audit_base_info ALL DONE !!")
    ]]
end

function audit.audit_pay_user()
    logger.debug("audit.audit_pay_user")
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
                                local update_info = {}
                                update_info.gameCardRecharge = user_info.gameCardRecharge
                                update_info.rechargeCount = user_info.rechargeCount
                                update_info.goldCoinRecharge = user_info.goldCoinRecharge
                                user.set_user_info(v.userID, update_info)
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

function audit.audit_ipcheck()
    logger.debug("audit_ipcheck")
    local city_name = require "city_name"
    
    local rkey = "pay_user_ip:"
    --local r = redis:get(rkey)
    local allIpCounter = {} 
    local noIpCount = 0
    local order_tables = {"OnlinePayNotify2017", "OnlinePayNotify2018", "OnlinePayNotify2019"}
    local count = 10
    local ipcheckID = "ipcheckID"
    local lastID = redis:get(ipcheckID) or 0
    for k, tname in pairs(order_tables) do
        while true do
            local sql = string.format("select * from %s where ID > %s order by ID asc limit %s", tname, lastID, count)
            local res = mysql_aux.localhost.exec_sql(sql)
            if not (res and next(res)) then
                break
            else
                for _, info in pairs(res) do
                    local payTime = futil.getTimeByDate(info.notifyTime)
                    local yearStr = os.date("%Y", payTime)
                    local ipCounter = allIpCounter[yearStr] or {}
                    local rkey = rkey..tostring(info.userID)
                    local ipName = redis:get(rkey)
                    if ipName then
                        local isFound = false
                        for prov, citys in pairs(city_name) do
                            local flag = false
                            if citys and next(citys) then
                                if string.find(ipName, prov) then
                                    --logger.err("not match :%s", ipName)
                                    for _, city in pairs(citys) do
                                        if string.find(ipName, city) then
                                            local lable = prov..":"..city
                                            ipCounter[lable] = ipCounter[lable] or 0
                                            ipCounter[lable] = ipCounter[lable] + 1
                                            flag = true
                                            break
                                        end
                                    end
                                    if not flag then
                                        local arr = futil.split(ipName, " ")
                                        local lable = prov..":"..arr[2]
                                        ipCounter[lable] = ipCounter[lable] or 0
                                        ipCounter[lable] = ipCounter[lable] + 1
                                    end
                                end
                            else
                                if string.find(ipName, prov) then
                                    local lable = prov..":"..prov
                                    ipCounter[lable] = ipCounter[lable] or 0
                                    ipCounter[lable] = ipCounter[lable] + 1
                                    flag = true
                                end
                            end
                            if flag then
                                break
                            end
                        end
                    else
                        noIpCount = noIpCount + 1
                    end
                    allIpCounter[yearStr] = ipCounter
                end
                lastID = res[#res].ID
                --redis:set(ipcheckID, lastID)
                break
            end
        end
    end
    for yearStr, counters in pairs(allIpCounter) do
        local rkey = "ip_counter:"
        rkey = rkey..yearStr
        redis_aux.db(2):del(rkey)
        for k, v in pairs(counters) do
            logger.debug("%s %s %s", yearStr, k, v)
            redis_aux.db(2):zadd(rkey, v, k)
        end
    end
    logger.debug("noIP count:%s", noIpCount)
    logger.debug("audit_ipcheck done")

end

function audit.audit_test()
    logger.debug("audit.audit_test")
    local rank_user = require "recharge_rank" 
    if not (rank_user and next(rank_user)) then
        logger.err("rank user empty")
        return
    end
    --[[
    local num = 0
    for userID, amount in pairs(rank_user) do
        local uif = user.get_user_info(userID) or {}
        uif.winCount = uif.winCount or 0
        uif.loseCount = uif.loseCount or 0
        if uif.winCount == 0 and uif.loseCount == 0 then
            num = num + 1
        end
    end
    logger.debug("total:10000, un process:%s", num)
    ]]
    local r = redis_aux.db(1):set("a", "aaaaaa")
    r = redis_aux.db(2):set("a", "bbbbbbbbbb")
    r = redis_aux.db(1):get("a")
    logger.debug("redis:1 a:%s", r)
    r = redis_aux.db(2):get("a")
    logger.debug("redis:2 a:%s", r)
    logger.debug("audit_test done !")
end

function audit.audit_active_day()
    logger.debug("audit.audit_active_day")
    local begin_time = futil.getTimeByDate("2017-07-01 00:00:00")
    local end_time = futil.getTimeByDate("2020-01-01 00:00:00")
    local prefix = "UserLog_"
    local val_time = begin_time
    local rank_user = require "recharge_rank"
    while true do
        local tname = prefix..futil.monthStr(val_time, "_")
        local db = nil
        for k, conf in pairs(mysql_conf) do
            local dbname = conf.database
            if dbname == 'Zipai' and conf.type == 1 then
                if is_table_exists(k, dbname, tname) then
                    logger.debug('table:%s exists in %s', dbname.."."..tname, k)
                    db = k       
                    break
                end
            end
        end
        if db then
            logger.debug("do query in :%s,%s", db, tname)
            local lastID = 0
            local count = 10000
            local active_counter = {}
            while true do
                local sql = string.format("select * from %s where ID > %s order by ID asc limit %s", tname, lastID, count)
                local _t = os.time()
                local rv = mysql_aux[db].exec_sql(sql)
                if rv.badresult then
                    logger.warn("table not exists maybe:%s %s", tname, db)
                    break
                end
                if rv and next(rv) then
                    logger.debug("query %s row %s.%s fromID:%s to %s take time:%s sec", count, db, tname, lastID, rv[#rv].ID, os.time() - _t)
                    -- do calc active day
                    for k, v in pairs(rv) do
                        if rank_user[v.userID] then
                            user_counter = active_counter[v.userID] or {}
                            local loginTime = futil.getTimeByDate(v.time)
                            local dayStr = futil.dayStr(loginTime)
                            if not user_counter[dayStr] then
                                user_counter[dayStr] = true
                                active_counter[v.userID] = user_counter
                                --logger.debug("user_counter:%s", table.tostring(user_counter))
                                --logger.debug("active_counter:%s", table.tostring(active_counter))
                            end
                        end
                    end
                    lastID = rv[#rv].ID
                else
                    logger.warn("no data found on table:%s %s", tname, db)
                    break
                end
                skynet.sleep(100)
            end
            --更新本月活跃天数到redis
            for userID, amount in pairs(rank_user) do
                local num = 0
                local user_counter = active_counter[userID] or {}
                for k, v in pairs(user_counter) do
                    num = num + 1
                end
                if num > 0 then
                    local user_info = user.get_user_info(userID) or {}
                    local update_info = {}
                    update_info.activeDay = user_info.activeDay or 0
                    update_info.activeDay = update_info.activeDay + num
                    user.set_user_info(userID, update_info)
                    logger.debug("add user:%s active day:%s", userID, num)
                end
            end
        else
            logger.err("table not exists:%s", tname)
        end
        val_time = futil.get_next_month(val_time)
        if val_time >= end_time then
            break
        end
    end
    logger.debug("calc_active_day done !")
end
function audit.audit_clear_game_win_lose()
    local rank_user = require "recharge_rank" 
    if not (rank_user and next(rank_user)) then
        logger.err("rank user empty")
    end
    for userID, amount in pairs(rank_user) do
        local rkey = const.redis_key.game_win..":"..userID
        redis:set(rkey, 0)
        local rkey = const.redis_key.game_lose..":"..userID
        redis:set(rkey, 0)
    end
    logger.debug("clear game win lose success")
end
--另一种统计前10000R输赢局数方法
function audit.audit_game_win_lose_2()
    logger.debug("audit.audit_game_win_lose_2")
    local table_prefix = {"GameUserInfoLog", "MatchUserInfoLog", "NewRoomSelfUserInfoLog"}
    local begin_time = futil.getTimeByDate("2017-03-05 00:00:00")
    local end_time = futil.getTimeByDate("2020-01-01 00:00:00")
    local rank_user = require "recharge_rank" 
    if not (rank_user and next(rank_user)) then
        logger.err("rank user empty")
    end
    for _, prefix in pairs(table_prefix) do
        local val_time = begin_time
        while true do
            local tname = string.format("%s_%s", prefix, futil.dayStr(val_time, "_"))
            local db = nil
            for k, conf in pairs(mysql_conf) do
                local dbname = conf.database
                if dbname == 'GameLog' and conf.type == 1 then
                    if is_table_exists(k, dbname, tname) then
                        logger.debug('table:%s exists in %s', dbname.."."..tname, k)
                        db = k       
                        break
                    end
                end
            end
            if db then
                logger.debug("query from table %s.%s", db, tname)
                local cursor = string.format("%s:%s", const.redis_key.game_win_lose_cursor, tname)
                local lastID = redis:get(cursor) or 0
                local count = 10000
                local _t = os.time()
                while true do
                    logger.debug("query table:%s from ID:%s", db.."."..tname, lastID)
                    local _t = os.time()
                    local sql = string.format("select * from %s where ID > %s and userID >= 5000 order by ID asc limit %s", tname, lastID, count)
                    local rv = mysql_aux[db].exec_sql(sql)
                    if rv.badresult then
                        logger.err("table may not exists:%s.%s", db, tname)
                        break
                    end
                    if rv and next(rv) then
                        for _, gameLog in pairs(rv) do
                            if rank_user[gameLog.userID] then
                                if gameLog.state == '赢' then
                                    local rkey = const.redis_key.game_win..":"..gameLog.userID
                                    redis:incr(rkey)
                                elseif gameLog.state == '输' then
                                    local rkey = const.redis_key.game_lose..":"..gameLog.userID
                                    redis:incr(rkey)
                                end
                            end
                            redis:set(cursor, gameLog.ID)
                        end
                        lastID = rv[#rv].ID
                        logger.debug('update game_win_lose_cursor to :%s', lastID)
                    else
                        break
                    end
                    logger.debug("query 10000 row take time:%s", os.time() - _t)
                    skynet.sleep(100)
                end
            else
                logger.err("table %s not exists", tname)
            end
            --move to next week
            val_time = val_time + 86400*7
            if val_time > end_time then
                break
            end
        end
    end
    
    logger.debug("audit_game_win_lost done !!")
end
--牌局排行：每天top100 
function audit.audit_game_record_rank()
    logger.debug("audit.audit_game_record_rank")
    local table_prefix = {"GameUserInfoLog", "MatchUserInfoLog", "NewRoomSelfUserInfoLog"}
    local begin_time = futil.getTimeByDate("2017-03-05 00:00:00")
    local end_time = futil.getTimeByDate("2020-01-01 00:00:00")
    
    --for _, prefix in pairs(table_prefix) do
    local val_time = begin_time
    while true do
        for _, prefix in pairs(table_prefix) do
            local tname = string.format("%s_%s", prefix, futil.dayStr(val_time, "_"))
            local db = nil
            for k, conf in pairs(mysql_conf) do
                local dbname = conf.database
                if dbname == 'GameLog' and conf.type == 1 then
                    if is_table_exists(k, dbname, tname) then
                        logger.debug('table:%s exists in %s', dbname.."."..tname, k)
                        db = k       
                        break
                    end
                end
            end
            if db then
                logger.debug("query from table %s.%s", db, tname)
                local cursor = string.format("%s:%s", const.redis_key.game_log_cursor, tname)
                local lastID = redis:get(cursor) or 0
                local count = 10000
                local _t = os.time()
                while true do
                    logger.debug("query table:%s from ID:%s", db.."."..tname, lastID)
                    local _t = os.time()
                    local sql = string.format("select * from %s where ID > %s and userID >= 5000 order by ID asc limit %s", tname, lastID, count)
                    local rv = mysql_aux[db].exec_sql(sql)
                    if rv.badresult then
                        logger.err("table may not exists:%s.%s", db, tname)
                        break
                    end
                    if rv and next(rv) then
                        for _, gameLog in pairs(rv) do
                            local logTime = futil.getTimeByDate(gameLog.startTime)
                            local incrKey = string.format("%s:%s", const.redis_key.game_record_rank, futil.dayStr(logTime))
                            redis:zincrby(incrKey, 1, gameLog.userID)
                            --更新游标
                            redis:set(cursor, gameLog.ID)
                        end
                        lastID = rv[#rv].ID
                        logger.debug("update lastID to:%s", lastID)
                    else
                        break
                    end
                    logger.debug("query 10000 row take time:%s", os.time() - _t)
                    skynet.sleep(100)
                end
            else
                logger.err("table %s not exists", tname)
            end
        end
        --move to next week
        val_time = val_time + 86400*7
        if val_time > end_time then
            break
        end
    end
    --end
    
    logger.debug("audit_game_record_rank done !!")
end

function audit.audit_game_win_lose()
    logger.debug("audit.audit_game_win_lose")
    local table_prefix = {"GameUserInfoLog", "MatchUserInfoLog", "NewRoomSelfUserInfoLog"}
    local begin_time = futil.getTimeByDate("2017-03-05 00:00:00")
    local end_time = futil.getTimeByDate("2020-01-01 00:00:00")
    local rank_user = require "recharge_rank" 
    if not (rank_user and next(rank_user)) then
        logger.err("rank user empty")
    end
    for userID, amount in pairs(rank_user) do
        local winCount = 0
        local loseCount = 0
        local uif = user.get_user_info(userID) or {}
        uif.winCount = uif.winCount or 0
        uif.loseCount = uif.loseCount or 0
        if uif.winCount == 0 and uif.loseCount == 0 then
            for _, prefix in pairs(table_prefix) do
                local val_time = begin_time
                while true do
                    local tname = string.format("%s_%s", prefix, futil.dayStr(val_time, "_"))
                    local db = nil
                    for k, conf in pairs(mysql_conf) do
                        local dbname = conf.database
                        if dbname == 'GameLog' and conf.type == 1 then
                            if is_table_exists(k, dbname, tname) then
                                logger.debug('table:%s exists in %s', dbname.."."..tname, k)
                                db = k       
                                break
                            end
                        end
                    end
                    if db then
                        logger.debug("query user:%s from table %s.%s", userID, db, tname)
                        local lastID = 0
                        local count = 10000
                        local _t = os.time()
                        while true do
                            local sql = string.format("select * from %s where userID=%s and ID > %s order by ID asc limit %s", tname, userID, lastID, count)
                            local rv = mysql_aux[db].exec_sql(sql)
                            if rv.badresult then
                                logger.err("table may not exists:%s.%s", db, tname)
                                break
                            end
                            if rv and next(rv) then
                                for _, gameLog in pairs(rv) do
                                    if gameLog.state == '赢' then
                                        winCount = winCount + 1
                                    elseif gameLog.state == '输' then
                                        loseCount = loseCount + 1
                                    end
                                end
                                lastID = rv[#rv].ID
                            else
                                break
                            end
                            skynet.sleep(100)
                        end
                        logger.debug("query user:%s from table:%s done, winCount:%s, loseCount:%s, take time:%s sec", userID, tname, winCount, loseCount, os.time() - _t)
                    else
                        logger.err("table %s not exists", tname)
                    end
                    --move to next week
                    val_time = val_time + 86400*7
                    if val_time > end_time then
                        break
                    end
                end
            end
            logger.debug("audit user:%s win lose done, winCount:%s, loseCount:%s", userID, winCount, loseCount)
            local update_info = {
                winCount = winCount,
                loseCount = loseCount,
            }
            user.set_user_info(userID, update_info)
        end
    end
    logger.debug("audit_game_win_lost done !!")
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
                                local update_info = {}
                                update_info.gameCardRecharge = user_info.gameCardRecharge
                                update_info.rechargeCount = user_info.rechargeCount
                                update_info.goldCoinRecharge = user_info.goldCoinRecharge
                                user.set_user_info(v.userID, update_info)
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

--从redis导出第一张表
function audit.audit_export_1()
    local rank_user = require "recharge_rank"
    if not (rank_user and next(rank_user)) then
        logger.err("rank_user not found")
        return
    end
    local failCount = 0
    local succCount = 0
    for userID, n in pairs(rank_user) do
        local rdsKey = string.format("%s:%s", const.redis_key.audit_user, userID)
        local user_info = user.get_user_info(userID) 
        if user_info then
            local sql = mysql_aux.get_insert_sql("audit_export_1", user_info)
            local rv = mysql_aux['export'].exec_sql(sql)
            if rv.badresult then
                logger.err("export user:%s fail:%s, sql:%s", userID, futil.toStr(rv), sql)
                failCount = failCount + 1
            else
                logger.debug("export user:%s success", userID)
                succCount = succCount + 1
            end
        end
    end
    logger.debug("audit export 1 DONE ,succ:%s, fail:%s", succCount, failCount)
end

local function format_output_string(str)
    if type(str) ~= 'string' then
        return str
    end
    local format_len = 24
    while #str < format_len do
        str = str.." "
    end
    return str
end

function audit.audit_recharge_detail()
    logger.debug("audit.audit_recharge_detail")
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
    local filename = "../out/recharge_detail.txt" 
    local sep = " | "
    local outfile = io.open(filename, "w")
    local title = format_output_string("账号ID")..sep..format_output_string("名称")..sep..format_output_string("充值时间")..sep..format_output_string("充值金额").."\n"
    outfile:write(title)
    if not outfile then
        logger.err("open %s fail", filename)
        return
    end
    local user_cache = {}
    for k, tbname in pairs(order_tables) do
        logger.debug('audit_recharge_detail deal with table:%s', tbname)
        while true do
            local _t = os.time()
            logger.debug('audit_recharge_detail query from ID:%s,table:%s', lastID, tbname)
            local sql = string.format("select * from %s where ID > %s order by ID asc limit 10000",
            tbname, lastID) 
            local res = mysql_aux.localhost.exec_sql(sql)
            if not (res and next(res)) then
                break
            else
                for k, v in pairs(res) do
                    if rank_user[v.userID] then
                        if not user_cache[v.userID] then
                            local sql = string.format("select * from User where ID=%s", v.userID)
                            local db_user = mysql_aux['2019_118'].exec_sql(sql)
                            if db_user.badresult then
                                logger.err("get user info fail from db:%s,%s", v.userID, futil.toStr(db_user))
                                db_user = {code = "null"}
                            else
                                if not (db_user and next(db_user)) then
                                    db_user = {code = "null"}
                                else
                                    user_cache[v.userID] = db_user[1]
                                end
                            end
                        end
                        local txt = format_output_string(tostring(user_cache[v.userID].code))..sep..format_output_string(user_cache[v.userID].nickName)..sep..format_output_string(v.notifyTime)..sep..format_output_string(tostring(v.totalFee)).."\n"
                        outfile:write(txt)
                    end
                end
                outfile:flush()
                lastID = res[#res].ID
                logger.debug("update lastID to:%s", lastID)
            end
            logger.debug('audit_recharge_detail deal with 10000 row take time:%s sec', os.time() - _t)
            skynet.sleep(100)
        end
        logger.debug('deal with table:%s end', tbname)
    end
    outfile:flush()
    outfile:close()
    local end_t = os.time()
    logger.debug("audit_recharge_detail done !! take time:%s sec", end_t - begin_t)
end

function audit.audit_goldcoin_log()
    logger.debug("audit_goldcoin_log")
    local rank_user = require "recharge_rank"
    if not (rank_user and next(rank_user)) then
        logger.err("rank user empty, returned")
        return
    end

    local tmp = {}
    for userID, amount in pairs(rank_user) do
        table.insert(tmp, {userID = userID, amount = amount})
    end
    table.sort(tmp, function(a, b)
        return a.amount > b.amount
    end)
    local begin_time = futil.getTimeByDate("2017-01-01 00:00:00")
    local end_time = futil.getTimeByDate("2020-01-01 00:00:00")
    local table_prefix = {"GoldCoinLog","RoomCardLog"}
    --分隔符
    local filename = string.format("../out/goldCoin_roomCard_log.txt") 
    local outfile = io.open(filename, "w")
    local sep = " | "
    local title = format_output_string("账号ID")..sep..format_output_string("名称")..sep..format_output_string("消耗时间")..sep..format_output_string("消耗币种")..sep..format_output_string("消耗数量").."\n"
    outfile:write(title)
    for i=1, 100 do
        local u = tmp[i]
        if u then
            local userID = u.userID
            if not outfile then
                logger.err("open out file fail:%s", filename)
                break
            end
            logger.debug("begin export goldcoin log userID:%s, amount:%s", u.userID, u.amount)
            local sql = string.format("select * from User where ID=%s", userID)
            local db_user = mysql_aux['2019_118'].exec_sql(sql)
            if db_user.badresult then
                logger.err("get user info fail from db:%s,%s", userID, futil.toStr(db_user))
                db_user = {code = "null"}
            else
                if not (db_user and next(db_user)) then
                    db_user = {code = "null"}
                else
                    db_user = db_user[1]
                end
            end
            local val_time = begin_time
            while true do
                for _, prefix in pairs(table_prefix) do
                    local tname = string.format("%s_%s", prefix, futil.monthStr(val_time, "_"))
                    local db = nil
                    for k, conf in pairs(mysql_conf) do
                        local dbname = conf.database
                        if dbname == 'Zipai' and conf.type == 1 then
                            if is_table_exists(k, dbname, tname) then
                                logger.debug('table:%s exists in %s', dbname.."."..tname, k)
                                db = k       
                                break
                            end
                        end
                    end
                    if db then
                        logger.debug("query user:%s gold coin log from %s.%s", userID, db, tname)
                        local lastID = 0
                        local count = 10000
                        while true do
                            local sql = string.format("select * from %s where userID=%s and ID > %s order by ID asc limit %s", tname, userID, lastID, count)
                            logger.debug("query %s.%s from ID:%s", db, tname, lastID)
                            local rv = mysql_aux[db].exec_sql(sql)
                            if rv.badresult then
                                logger.err("table %s.%s may not exists skip", db, tname)
                                break
                            end
                            if rv and next(rv) then
                                --deal with gold coin log
                                for k, v in pairs(rv) do
                                    if v.changeCurrency < 0 and (v.goodsID == 107 or v.goodsID == 0) then
                                        local goodsName = "金币"
                                        if v.goodsID == 107 then
                                            goodsName = "对战卡"
                                        end
                                        local txt = format_output_string(tostring(db_user.code))..sep..format_output_string(v.nickName)..sep..format_output_string(v.time)..sep..format_output_string(goodsName)..sep..format_output_string(tostring(v.changeCurrency)).."\n"
                                        --logger.debug("write to file:%s", txt)
                                        outfile:write(txt)
                                    end
                                    lastID = rv[#rv].ID
                                end
                            else
                                logger.debug("query user:%s from table %s.%s done", userID, db, tname)
                                break
                            end
                            skynet.sleep(100)
                        end
                    end
                end
                val_time = futil.get_next_month(val_time)
                if val_time >= end_time then
                    break
                end
            end
            outfile:flush()
        end
    end
    outfile:close()
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

function CMD.audit_msg(session, ch, msg)
    logger.debug("recv audit_msg:%s,%s", ch, msg)
    if ch == const.pubsubChannel.ch_audit then
        logger.debug("audit_msg query db test start")
        local rv = mysql_aux[msg].exec_sql("select version()")
        if rv and next(rv) then
            logger.debug("rv:%s", futil.toStr(rv))
            logger.debug("audit_msg query db test success")
        else
            logger.err("audit_msg query db test fail")
        end
    end
end

skynet.init(function()
    CMD.init()
    redis.sub(const.pubsubChannel.ch_audit, "audit_msg")
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

