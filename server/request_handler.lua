local logger = require "logger"
local urllib = require "http.url"
local futil = require "futil"
local request_handler = {}

local function root(method, header, query, body)
    logger.debug('root query:%s, body:%s', query, body)
    return "OKxx"
end

local function alipay_trade_precreate(method, header, query, body)
    logger.debug('alipay_trade_precreate:query:%s,body:%s', query, body)
    return "OKxx"
end
local function query_to_table(query)
	local res = {}
	local arr = futil.split(query, "&")
	if arr and next(arr) then
		for _, item in pairs(arr) do
			local kv = futil.split(item, "=")
			if #kv == 2 then
				res[kv[1]] = kv[2]
			end
		end
	end
	return res
end
local function wechat_auth_callback(method, header, query, body)
    logger.debug('wechat_auth_callback:query:%s,body:%s', query, body)
	logger.debug("q:%s", futil.toStr(query_to_table(query)))
	local request = query_to_table(query)
	return request.echostr 
end
local path_handler = {
    ["/"] = root,
	["/wechat_auth_callback"] = wechat_auth_callback,
    ["/payment_notify/alipay_trade_precreate"] = alipay_trade_precreate, 
}
function request_handler.handle_request(path, method, header, query, body)
    local f = path_handler[path] 
    if f then
        return f(method, header, query, body)
    else
        return "no path hander found"
    end
end

return request_handler 
