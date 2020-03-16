local skynet = require "skynet"
local redis = require "redis_utils"
local sqlaux = {}
local logger = log4.get_logger(SERVICE_NAME)
local common = require "common"
local mysql = require "skynet.db.mysql"
local mt_lock = require "mt_lock"
local lock = require "lock"
local lock_k = "sqlaux_init"
local worker_id = tonumber(skynet.getenv("nodeid")) or 0
local mqv = function(v)
    return mysql.quote_sql_str(v)
end
local mysql_agent = {}
local db_idx = 1
skynet.init(function()
    lock.lock(lock_k) 
    skynet.timeout(100, function()
        mysql_agent = skynet.call(constants.unique_services.mysql.name, "lua", 
        "get_all_agent")
        if not (mysql_agent and next(mysql_agent)) then
            logger.fatal("sqlaux get mysql_agent failed")
        end
        lock.release(lock_k)
        if #mysql_agent <= 1 then
            logger.fatal("请把mysql连接数设置大于等于2")
        end
    end)
end)
local function get_mysql_agent(idx)
    if not (mysql_agent and next(mysql_agent)) then
        while true do
            if not lock.lock(lock_k) then
                lock.wait(lock_k)
            else
                lock.release(lock_k)
                break
            end
        end
    end
    if not (mysql_agent and next(mysql_agent)) then
        logger.fatal("sqlaux get_mysql_agent failed, mysql_agent empty")
        return nil 
    end
    local ag = nil
    if idx then
        ag = mysql_agent[idx]
    else
        ag = mysql_agent[db_idx]
        db_idx = db_idx + 1
        if db_idx > #mysql_agent then
            db_idx = 1
        end
    end
    return ag
end
--执行事务sql
local function trans_exec(sql)
    local data = skynet.call(".trans_service", "lua", "exec_sql",sql)
    if not data then
        return false
    end
    if data.badresult == true then
        return false, data
    end
    return true, data
end
--开始事务
function sqlaux.begin()
    local k = string.format("%s:%s", common.lock_key.mysql_trans, worker_id)
    local cur_t = os.time()
    mt_lock.lock_wait(k)
    if os.time() - cur_t > 30 then
        mt_lock.release(k)
        logger.error('begin trans timeout , please try again')
        return false
    end
    local ok, ret = trans_exec("begin")
    if ok then
        return true, trans_exec
    end
    return false
end
--回滚事务
function sqlaux.rowback()
    local k = string.format("%s:%s", common.lock_key.mysql_trans, worker_id)
    local ok, ret = trans_exec("rollback")
    mt_lock.release(k)
    return ok, ret
end
--提交事务
function sqlaux.commit()
    local k = string.format("%s:%s", common.lock_key.mysql_trans, worker_id)
    local ok, ret = trans_exec("commit")
    mt_lock.release(k)
    return ok, ret
end
function sqlaux.get_insert_sql(tablename, data)
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
function sqlaux.get_delete_sql(tablename, keys)
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
function sqlaux.get_query_sql(tablename, keys, fields, ext)
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
function sqlaux.get_update_sql(tablename, keys, data)
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
function sqlaux.exec_sql(sql)
    local ag = get_mysql_agent()
    if ag then
        local data = skynet.call(ag, "lua", "db_query","",sql)
        if data.badresult == true then
            return false, data
        end
        return true, data
    end
    return false
end

function sqlaux.insert(tname, uid, pkey, data)
	uid = tostring(uid)
	if not tname then
		return false
	end
	if type(data) ~= 'table' then
		return false
	end
	--insert into mysql
	local sql = sqlaux.get_insert_sql(tname, data)
	local ok, dbres = sqlaux.exec_sql(sql)
	if not ok then
		logger.error("sqlaux.insert sql error:%s,%s", sql, table.tostring(dbres))
		return false
	end
	--write to redis
	local rkey = tname..":"..uid
	local subKey = pkey 
	local sets = {rkey..":"..subKey}
	for k, v in pairs(data) do
		table.insert(sets, k)
		table.insert(sets, v)
	end
	local r = redis:hmset(sets)
	redis:expire(rkey, common.redis_ttl.cache_timeout)
	logger.debug('sqlaux.insert redis:%s', r)
	return true
end

function sqlaux.update(tname, uid, kname, kvalue, data, extkv)
	if not (tname and uid and kname and kvalue) then
		logger.error("sqlaux.update: params error")
		return false
	end
	if not (data and next(data)) then
		logger.error("sqlaux.update: data nil")
		return false
	end
	local kv = {}
	kv[kname] = kvalue
	if extkv and next(extkv) then
		--有额外的key
		for k, v in pairs(extkv) do
			kv[k] = v
		end
	end
	local sql = sqlaux.get_update_sql(tname, kv, data)
	local ok, dbres = sqlaux.exec_sql(sql)
	if not ok then
		logger.error("sqlaux update %s failed, kname:%s,kvalue:%s,%s", 
			tname, kname, kvalue, table.tostring(dbres))
		return false
	end
	if dbres.affected_rows <= 0 then
		-- no row match 
		return false
	end
	-- update redis
	local rkey = tname..":"..uid..":"..kvalue
	local sets = {rkey}
	for k, v in pairs(data) do
		table.insert(sets, k)
		table.insert(sets, v)
	end
	local r = redis:hmset(sets)
	redis:expire(rkey, common.redis_ttl.cache_timeout)
	logger.debug('update redis:%s success', rkey)
	return true
end

function sqlaux.delete(tname, uid, kname, kvalue)
	if not (tname and uid and kname and kvalue) then
		return false
	end
	local kv = {}
	kv[kname] = kvalue
	local sql = sqlaux.get_delete_sql(tname, kv)
	local ok, dbres = sqlaux.exec_sql(sql)
	if not ok then
		return false
	end
	--delete redis
	local rkey = tname..":"..uid..":"..kvalue
	local r = redis:del(rkey)
	return true
end

function sqlaux.query(tname, uid, kname, kvalue)
	local ret = nil
	if not tname then
		return ret 
	end
	local rkey = tname..":"..uid..":"..kvalue
	local rs = redis:hgetall(rkey)
	if #rs == 0 then
		--query from db
		local kv = {}
		kv[kname] = kvalue
		local sql = sqlaux.get_query_sql(tname, kv, {})
		local ok, dbres = sqlaux.exec_sql(sql)
		if ok then
			ret = dbres[1]
			if ret and next(ret) then
				-- update to redis
				local sets = {rkey}
				for k, v in pairs(ret) do
					table.insert(sets, k)
					table.insert(sets, v)
				end
				redis:hmset(sets)
			end
		end
	else
		ret = {}
		for i=1, #rs, 2 do
			if not common.user_info_not_to_number[rs[i]] then
			   ret[rs[i]] = tonumber(rs[i+1]) or rs[i+1]
			else
			   ret[rs[i]] = rs[i+1]
			end
		end
	end

	return ret
end

---------------------------------- core -----------------------------------
local yeepay_helper = require "yeepay_helper"
local uuid_util = require "uuid_util"
--@param uid :uid 可以是userid或account,但各个增删改查必需一致

function sqlaux.get_yeepay_recharge_order(order_id)
	--uid 只作为redis的一个subkey,不存数据库,0表示通用的subkey,即不区分subkey
	local uid = 0
	if not (uid and order_id) then
		return nil
	end
	local tname = "yeepay_recharge_"..uuid_util.get_id_ym(order_id)
	local orderInfo = sqlaux.query(tname, uid, "order_id", order_id)
	return orderInfo
end

function sqlaux.insert_yeepay_recharge_order(orderInfo)
	local uid = 0
	if not (orderInfo and orderInfo.order_id) then
		return false
	end
	local order_id = orderInfo.order_id
	local tname = "yeepay_recharge_"..uuid_util.get_id_ym(order_id)
	return sqlaux.insert(tname, uid, order_id, orderInfo)
end

function sqlaux.update_yeepay_recharge_order(orderInfo, exterKs)
	local uid = 0
	local order_id = orderInfo.order_id
	if not (orderInfo and order_id) then
		return false
	end
	update_fs = {}
	for k, v in pairs(orderInfo) do
		if k ~= "order_id" then
			update_fs[k] = v
		end
	end
	local tname = "yeepay_recharge_"..uuid_util.get_id_ym(order_id)
	return sqlaux.update(tname, uid, "order_id", order_id, update_fs, exterKs)
end

function sqlaux.delete_yeepay_recharge_order(order_id)
	local uid = 0
	if not (order_id and uid) then
		return false
	end
	local tname = "yeepay_recharge_"..uuid_util.get_id_ym(order_id)
	return sqlaux.delete(tname, uid, "order_id", order_id)
end
--
-- yeepay withdraw order 
--
function sqlaux.get_yeepay_withdraw_order(order_id)
	local uid = 0
	if not (uid and order_id) then
		return nil
	end
	local tname = "yeepay_outrecharge_"..uuid_util.get_id_ym(order_id)
	local orderInfo = sqlaux.query(tname, uid, "order_id", order_id)
	return orderInfo
end

function sqlaux.insert_yeepay_withdraw_order(orderInfo)
	local uid = 0
	if not (orderInfo and orderInfo.order_id) then
		return false
	end
	local order_id = orderInfo.order_id
	local tname = "yeepay_outrecharge_"..uuid_util.get_id_ym(order_id)
	return sqlaux.insert(tname, uid, order_id, orderInfo)
end

function sqlaux.update_yeepay_withdraw_order(orderInfo, exterKs)
	local uid = 0
	local order_id = orderInfo.order_id
	if not (orderInfo and order_id) then
		return false
	end
	update_fs = {}
	for k, v in pairs(orderInfo) do
		if k ~= "order_id" then
			update_fs[k] = v
		end
	end
	local tname = "yeepay_outrecharge_"..uuid_util.get_id_ym(order_id)
	return sqlaux.update(tname, uid, "order_id", order_id, update_fs, exterKs)
end

function sqlaux.delete_yeepay_withdraw_order(order_id)
	local uid = 0
	if not (order_id and uid) then
		return false
	end
	local tname = "yeepay_outrecharge_"..uuid_util.get_id_ym(order_id)
	return sqlaux.delete(tname, uid, "order_id", order_id)
end

function sqlaux.get_user_limit(account)
    local tname = "user_limit"
    local id = 0
    local limit_info = sqlaux.query(tname, id, "account", account)
    return limit_info
end

--redpacket
function sqlaux.insert_redpacket(redpacket_id, data)
    local uid = 0
    local old_account, old_money, old_timestamp, old_num = redpacketid_parse(redpacket_id)
    local tname = "red_packet_"..getYearMonthStr(old_timestamp) 
    return sqlaux.insert(tname, uid, redpacket_id, data)
end

function sqlaux.update_redpacket(redpacket_id, data)
    local uid = 0
    local old_account, old_money, old_timestamp, old_num = redpacketid_parse(redpacket_id)
    local tname = "red_packet_"..getYearMonthStr(old_timestamp) 
    return sqlaux.update(tname, uid, "redpacket_id", redpacket_id, data)
end

function sqlaux.get_redpacket(redpacket_id)
    local uid = 0
    local old_account, old_money, old_timestamp, old_num = redpacketid_parse(redpacket_id)
    local tname = "red_packet_"..getYearMonthStr(old_timestamp) 
    return sqlaux.query(tname, uid, "redpacket_id", redpacket_id)
end

--
-- user dao 
--
function sqlaux.call_mysql_agent(cmd, ...)
    local ag = get_mysql_agent()
    if not ag then
        return false
    end
    return skynet.call(ag, "lua", cmd, ...)
end

function sqlaux.set_user_to_mysql(uid, fields)
    return sqlaux.call_mysql_agent("update_user", uid, fields)
end

function sqlaux.get_user_from_mysql(uid)
    local ok, user = sqlaux.call_mysql_agent("get_user", uid)
    return ok and user and user[1]
end

function sqlaux.get_user_by_account(account)
    local ok, user = sqlaux.call_mysql_agent("get_user_by_account", account)
    return ok and user and user[1]
end
function sqlaux.get_user_by_phone(phone)
    local ok, user = sqlaux.call_mysql_agent("get_user_by_phone", phone)
    return ok and user and user[1]
end
function sqlaux.get_user_by_user_code(code)
    local ok, user = sqlaux.call_mysql_agent("get_user_by_user_code", code)
    return ok and user and user[1]
end

function sqlaux.set_user_to_redis(uid, u)
    return redis.call_agent("set_user", uid, table_to_array(u))
end

function sqlaux.get_user_from_redis(uid)
    return redis.call_agent("get_user", uid)
end


function sqlaux.insert_yeepay_redpacket(redpacket_id, data)
    local uid = 0
   	local old_timestamp = os.date("%Y%m")
    local tname = "red_packet_yeepay_"..old_timestamp 
    return sqlaux.insert(tname, uid, redpacket_id, data)
end

function sqlaux.update_yeepay_redpacket(redpacket_id, data)
    local uid = 0
    local old_timestamp = os.date("%Y%m")
    local day = os.date("%d",skynet_time())
    local tname = "red_packet_yeepay_"..old_timestamp
    local result = sqlaux.update(tname, uid, "redpacket_id",redpacket_id,data)
    if (day == "01") and (not result)  then 
  		local s1 =get_last_month(time_now_str())
        local lastmonth =  string.format("%04d%02d", checkint(s1.year), checkint(s1.month))
        local tablename = string.format("red_packet_yeepay_%s",lastmonth)
        return sqlaux.update(tablename, uid, "redpacket_id",redpacket_id,data)
    else
    	return result
    end    
end

function sqlaux.get_yeepay_redpacket(redpacket_id)
    local uid = 0
    local old_timestamp = os.date("%Y%m")
    local day = os.date("%d",skynet_time())
    local tname = "red_packet_yeepay_"..old_timestamp
    local result = sqlaux.query(tname, uid, "redpacket_id", redpacket_id)
    if (day == "01") and (not result)  then 
  		local s1 =get_last_month(time_now_str())
        local lastmonth =  string.format("%04d%02d", checkint(s1.year), checkint(s1.month))
        local tablename = string.format("red_packet_yeepay_%s",lastmonth)
        return sqlaux.query(tablename, uid, "redpacket_id", redpacket_id),tostring(lastmonth)
    else
    	return result,old_timestamp
    end
end
return sqlaux
