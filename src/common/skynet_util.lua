local skynet = require "skynet"
local util = {}

function util.lua_docmd(cmdhandler, session, cmd, ...)
	local f = cmdhandler[cmd]
	if not f then
		return error(string.format("%s Unknown command %s", SERVICE_NAME, tostring(cmd)))
	end
	if session == 0 then
		return f(...)
	else
		return skynet.ret(skynet.pack(f(...)))
	end
end

function util.handle_err(e)
	e = debug.traceback(coroutine.running(), tostring(e), 2)
	skynet.error(e)
	return e
end

function util.diff_time(t1, t2)
	t2 = t2 or skynet.now()
	if t2 < t1 then return t2 + (0x100000000 - t1) end
	return t2 - t1
end

-- 保证被撤销任务的定时器在10秒内清除
local function schedule_sleep(id, t)
	local co = coroutine.running()
	if t <= 1000 then
		id.sleep = co
		skynet.sleep(t > 0 and t or 0)
		id.sleep = nil
	else
		while t > 1000 do
			local now = skynet.now()
			id.sleep = co
			skynet.sleep(math.random(100, 1000))	-- 以防定时器集中
			id.sleep = nil
			if id.canceled then return end
			t = t - util.diff_time(now)
		end
		if t > 0 then
			id.sleep = co
			skynet.sleep(t)
			id.sleep = nil
		end
	end
end

local function schedule_func(id, ...)
	if id.canceled then return end
	local t = id.first_interval or id.interval
	while true do
		schedule_sleep(id, t)
		if id.canceled then return end
		xpcall(id.callback, util.handle_err, id, ...)
		if id.canceled or not id.repeated then return end
		t = id.interval
	end
end

local function schedule_func_fixfrmt(id, ...)
	if id.canceled then return end
	local t = id.first_interval or id.interval
	while true do
		schedule_sleep(id, t)
		if id.canceled then return end
		local ct = skynet.now()
		xpcall(id.callback, util.handle_err, id, ...)
		if id.canceled or not id.repeated then return end
		t = id.interval - util.diff_time(ct)
	end
end

-- fixfrmt : 是否固定帧率，否则每次更新之后使用指定的固定时间间隔
-- first_interval : 第一次执行的时间间隔
-- Important Note : id will be passed to callback as the first paramater
function util.schedule(interval, callback, repeated, first_interval, fixfrmt, ...)
	local id = {interval = math.floor(interval), callback = callback, repeated = repeated, first_interval = first_interval}
	if fixfrmt then
		skynet.fork(schedule_func_fixfrmt, id, ...)
	else
		skynet.fork(schedule_func, id, ...)
	end
	return id
end

function util.unschedule(id)
	id.canceled = true
	if id.sleep then
		skynet.wakeup(id.sleep)
		id.sleep = nil
	end
end

function util.do_fail_retry(func, retry_interval, times, ...)
	times = times or -1
	retry_interval = retry_interval or 1000
	while true do
		local ret = table.pack(func(...))
		if ret[1] then
			return table.unpack(ret, 1, ret.n)
		end
		times = times - 1
		skynet.error(string.format("DO_FAIL_RETRY FAIL: left times=%d, error:%s", times, ret[2]))
		if times == 0 then
			return false
		end
		skynet.sleep(retry_interval)
	end	
end

-- func: function(...)(bool, res), return true to end retry
-- retry_interval: 1/100 second
-- times: retry times, -1 is forever
function util.create_fail_retry(func, retry_interval, times)
	times = times or -1
	retry_interval = retry_interval or 1000
	return function(...)
		local t = times
		while true do
			local res = table.pack(func(...))
			if res[1] then
				return table.unpack(res, 1, res.n)
			end
			t = t - 1
			skynet.error(string.format("CREATED FAIL_RETRY FAIL: left times=%d, error:%s", t, res[2]))
			if t == 0 then
				return false
			end
			skynet.sleep(retry_interval)
		end
	end
end

--允许skynet.call超时时返回
--注意    : 返回值与skynet.call不同, 第一个值表示是否成功，第二个值开始是错误或者真正的结果
--argument: timeout 超时时间, 单位=1/100秒
--return  : ok, ...
function util.timeout_call(timeout, addr, typename, ...)
	if not (timeout and timeout > 0) then error("invalid argument: timeout") end
	local self_co = coroutine.running()
	local r
	skynet.fork(function(...)
		r = table.pack(pcall(skynet.call, addr, typename, ...))
		if not self_co then return end
		skynet.wakeup(self_co)
		self_co = nil
	end, ...)
	skynet.timeout(timeout, function ()
		if not self_co then return end
		skynet.wakeup(self_co)
		self_co = nil
	end)
	skynet.wait()
	if not r then
		return false, "request timeout"
	end
	return table.unpack(r, 1, r.n)
end

function util.string_to_handle(str)
	return tonumber("0x" .. string.sub(str, 2))
end

return util
