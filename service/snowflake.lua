local skynet = require "skynet"
require "skynet.manager"
require "tostring"
local logger = "logger" 
local CMD = {}
local global = {
	global_int = 0,
	last_stamp = 0,
	workid = 0,
	seqid = 0,
}
local sequence_mask = (-1 ^ (-1 << 12))
local function get_cur_ms()
	return math.floor(skynet.time()*1000)
end

local function wait_next_ms(laststamp)
	local cur = get_cur_ms()
	while cur <= laststamp do
		cur = get_cur_ms()
	end
	return cur
end

local function atomic_incr(id)
	return (id+1)
end

local function get_unique_id()
	local unique_id = 0
	local nowtime = get_cur_ms()
	unique_id = nowtime << 22
	unique_id = unique_id | ((global.workid & 0x3ff) << 12)

	if nowtime < global.last_stamp then
		return nil
	end
	if nowtime == global.last_stamp then
		global.seqid = atomic_incr(global.seqid)&sequence_mask 
		if global.seqid == 0 then
			nowtime = wait_next_ms(global.last_stamp)
		end
	else
		global.seqid = 0
	end
	global.last_stamp = nowtime
	unique_id = unique_id | global.seqid
	return unique_id
end

function CMD.get_unique_id()
	return get_unique_id()
end

--用数据库生成10位的user_code
function CMD.get_user_code()
	local sql = string.format([[insert into user_code(laststamp) values(%s)]], os.time())
	local data = skynet.call(constants.unique_services.db.name, "lua", "db_query", "", sql)
	if db_check(data) == false then
		return nil
	end
	logger.debug("get_user_code data:%s", table.tostring(data))
	return data and data.insert_id
end
local function init()
	skynet.register(".snowflake")
end
skynet.init(init)
skynet.start(function()
	skynet.dispatch("lua", function(session, addr, cmd, ...)
		local f = CMD[cmd]
		if f then
			skynet.ret(skynet.pack(f(...)))
		else
			logger.err("Unkown command:%s", tostring(cmd))
		end
	end)
end)
