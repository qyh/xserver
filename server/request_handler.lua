local logger = require "logger"
local urllib = require "http.url"
local futil = require "futil"
local skynet = require "skynet"
local json = require "cjson"
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
local function wechat_auth_redirect(method, header, query, body)
	local appid = "wx4d738365325b2dde" 
	local secret = "d50cff74e3b6bb5f439e89a9813ad3df"  
    logger.debug('wechat_auth_callback:query:%s,body:%s', query, body)
	local request = query_to_table(query)
	logger.debug("get code:%s", request.code)
	-- get access_token
	local host = "https://api.weixin.qq.com/sns/oauth2/access_token"
	params = {}
	params.appid = appid 
	params.secret = secret 
	params.code = request.code
	params.grant_type = "authorization_code"
	local ok, body = skynet.call(".webclient","lua", "request", host, nil, params)
	if not ok then
		logger.err("http request failed:%s", body)
	end
	logger.debug("access_token:%s", body)
	local token_info = json.decode(body)
	-- get userinfo
	host = "https://api.weixin.qq.com/sns/userinfo"
	params = {}
	params.access_token = token_info.access_token
	params.openid = token_info.openid
	ok, body = skynet.call(".webclient", "lua", "request", host, nil, params)
	if not ok then
		logger.err("request userinfo failed")
	end
	logger.debug("userinfo:%s", body)
	-- refresh token
	host = "https://api.weixin.qq.com/cgi-bin/token"
	params = {}
	params.grant_type = "client_credential"
	params.appid = appid
	params.secret = secret
	ok, body = skynet.call(".webclient", "lua", "request", host, nil, params)
	if not ok then
		logger.err("request userinfo failed")
	end
	logger.debug("refresh token:%s", body)
	return request.code or query 
end
local function wechat_auth_callback(method, header, query, body)
    logger.debug('wechat_auth_callback:query:%s,body:%s', query, body)
	logger.debug("query:%s", futil.toStr(query_to_table(query)))
	logger.debug("body:%s", body)
	local request = query_to_table(query)
	return request.echostr 
end
local path_handler = {
    ["/"] = root,
	["/wechat_auth_callback"] = wechat_auth_callback,
	["/wechat_auth_redirect"] = wechat_auth_redirect,
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
