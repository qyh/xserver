local skynet = require "skynet"
local httpc = require "http.httpc"
local md5 = require "md5"
local codec = require('codec')
local xmlser = require "xml-ser"
local json = require "cjson"
local const = require "const"
local futil = require "futil"
local url = require "http.url"
local cryptopp = require "cryptopp"
local logger = require "logger"
--local logger = require "userlog"
local APP_ID = skynet.getenv("lk_game_appid") or "28449289"
local APP_KEY = skynet.getenv("lk_game_appkey") or "d72101a1375f4cdbb058455b1bff7df3"
local LK_GAME_HOST = skynet.getenv("lk_game_host") or "api.lkgame.com"
local CMD = {}
local function handle_error(e)
	return debug.traceback(coroutine.running(), tostring(e), 2)
end
local alipay_config = {
    app_id = "2016080200153059", 
    seller_id = "widvej2444@sandbox.com",
    rsa_pub = 
[[-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDIgHnOn7LLILlKETd6BFRJ0GqgS2Y3mn1wMQmyh9zEyWlz5p1zrahRahbXAfCfSqshSNfqOmAQzSHRVjCqjsAw1jyqrXaPdKBmr90DIpIxmIyKXv4GGAkPyJ/6FTFY99uhpiq0qadD/uSzQsefWo0aTvP/65zi3eof7TcZ32oWpwIDAQAB
-----END PUBLIC KEY-----
]],
    rsa_pri = 
[[
-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDMtPYcbf59LGsom6uhq+1xpAJ1e7iqZph59atpCYnOfovnfTAV
fPZjXdEEzPMi+UCEcWEm2TG4SKpJJmgyvki1TVK67p8pY4hwEbTEjVApvMmr4kKw
XPCFvRpDchjbvjAUZ7723thQMWoEcZ9EdlmV743Gq38hxZVwAcQi0ptEAwIDAQAB
AoGBAK1thrCxU5Dy59pggHkY8rJ7dAXaiqn6/6UbyFvV0+WY+qhlPC6ITyoCGopJ
pJd1uf47HEbQbbol/fa5Tj/nTCqh2c5ZmhYPIyEkJTl2shyewrEdTYGZAS7JmZ7p
E4zJwsCti83LvEs9yr5CiLQ47KybKqQNhQVdNqx+6M/hlgExAkEA6hxiBwhEqCIk
zvGF5RpeKbXYA23w4abAWPDituo3mIcZR0HHrkxoknvmJSDlhkDKg5DSYQA1pqSJ
wEyF7GzmCwJBAN/YxayAU16RHL8r6zCddyOq6WN7kXgTVV20a6Hc2kLwrhJMtd/+
FZR0Z6F4EuGm6ZX1S3zj8q5JnYO9c8BcLOkCQQCzjmtdC2llLzLeCf5w9mVMRm1U
TAlZeMTEMpUgR8m2UcAAmCInu/DkkOS2i2GfM6hfej7xKPD9S+tfMxHwgKF7AkAD
9IqZn7LZaur9HcOMGlwujyiMj3RkkMLjYvq743Ef2azZue1ExfKPqvbhzYNX5WEf
OzRYQpbZKYfU+hX/giFxAkEAzRTcuoAAHXJ3RZ2zrEQHABprZVLTuZ+/gNapPxdm
dfQXZKzGlfGf1QAcbQ0/O/Lb9ZFk3Q6XJq1nZsmuFoZecA==
-----END RSA PRIVATE KEY-----    
]],
pkcs8 = [[
-----BEGIN PRIVATE KEY-----
MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAMy09hxt/n0sayib
q6Gr7XGkAnV7uKpmmHn1q2kJic5+i+d9MBV89mNd0QTM8yL5QIRxYSbZMbhIqkkm
aDK+SLVNUrrunyljiHARtMSNUCm8yaviQrBc8IW9GkNyGNu+MBRnvvbe2FAxagRx
n0R2WZXvjcarfyHFlXABxCLSm0QDAgMBAAECgYEArW2GsLFTkPLn2mCAeRjysnt0
BdqKqfr/pRvIW9XT5Zj6qGU8LohPKgIaikmkl3W5/jscRtBtuiX99rlOP+dMKqHZ
zlmaFg8jISQlOXayHJ7CsR1NgZkBLsmZnukTjMnCwK2Lzcu8Sz3KvkKItDjsrJsq
pA2FBV02rH7oz+GWATECQQDqHGIHCESoIiTO8YXlGl4ptdgDbfDhpsBY8OK26jeY
hxlHQceuTGiSe+YlIOWGQMqDkNJhADWmpInATIXsbOYLAkEA39jFrIBTXpEcvyvr
MJ13I6rpY3uReBNVXbRrodzaQvCuEky13/4VlHRnoXgS4abplfVLfOPyrkmdg71z
wFws6QJBALOOa10LaWUvMt4J/nD2ZUxGbVRMCVl4xMQylSBHybZRwACYIie78OSQ
5LaLYZ8zqF96PvEo8P1L618zEfCAoXsCQAP0ipmfstlq6v0dw4waXC6PKIyPdGSQ
wuNi+rvjcR/ZrNm57UTF8o+q9uHNg1flYR87NFhCltkph9T6Ff+CIXECQQDNFNy6
gAAdcndFnbOsRAcAGmtlUtO5n7+A1qk/F2Z19BdkrMaV8Z/VABxtDT878tv1kWTd
DpcmrWdmya4Whl5w
-----END PRIVATE KEY-----
]]
}
local function hex2bin(str)
	return str:gsub("..", function (s)
		return string.char(tonumber(s, 16))
	end)
end

local function bin2hex(str)
	return str:gsub(".", function (s)
		return string.format("%02x", string.byte(s))
	end)
end
local function getLkUserToken()
    local ec = const.error_code
    local ret = {
        openid = '',
        token = nil,
        errorCode = ec.ok,
        user_type = 0,
    }
    local header = {
        ["content-type"] = "application/x-www-form-urlencoced"
    }
    local recvheader = {} 
    local host = LK_GAME_HOST 
    local rand_num = 47
    local user_name = 'qin6174'
    local password = md5.sumhexa('qin17092070779')
    local partner = 0
    local source = 0
    local nickname = 'cplus'
    local sys_type = 0
    local sign = md5.sumhexa(APP_ID..APP_KEY..rand_num..user_name..password)
    local form = {
        appID = APP_ID,
        randNum = rand_num,
        userName = user_name,
        password = password,
        sign = sign,
    }
    local content = "" --json.encode(form)
    local uri = string.format("/Login/Do?appID=%s&randNum=%s&userName=%s&password=%s&sign=%s",
    APP_ID,rand_num,user_name,password,sign)
    -- perform http request
    local ok, status, body = pcall(httpc.request,"POST", host, uri, recvheader, header, content)
    if not ok then 
        skynet.error('http request failed')
        ret.errorCode = ec.http_req_fail
        return ret
    end
    if status ~= 200 then
        skynet.error('http request failed status:'..status)
        ret.errorCode = ec.http_req_fail
        return ret
    end
    if not body or body == '' then
        skynet.error('http data error')
        ret.errorCode = ec.http_data_error
        return ret
    end
    local ok, info = pcall(json.decode, body)
    if not ok then
        skynet.error('response data decode error')
        ret.errorCode = ec.http_data_error
        return ret
    end
    skynet.error('getLkUserOpenid:'..body)
    ret.openid = info.OpenID
    ret.token = info.Token
    ret.errorCode = info.RetCode
    return ret

end
local function validateLkUser(openid, token)
    local ec = const.error_code
    local ret = {
        errorCode = ec.ok,
    }
    local header = {
        ["content-type"] = "application/x-www-form-urlencoced"
    }
    local recvheader = {}
    local form = {
        uid = openid,
        token = token,
    }
    local content = ""--json.encode(form)
    local host = LK_GAME_HOST 
    local uri = string.format("/User/GetUserInfo?uid=%s&token=%s",openid,token)
    -- perform http request
    local ok, status, body = pcall(httpc.request,"POST", host, uri, recvheader, header, content)
    if not ok then 
        skynet.error('http request failed')
        ret.errorCode = ec.http_req_fail
        return ret
    end
    if status ~= 200 then
        skynet.error('http request failed status:'..status)
        ret.errorCode = ec.http_req_fail
        return ret
    end
    if not body or body == '' then
        skynet.error('http data error')
        ret.errorCode = ec.http_data_error
        return ret
    end
    skynet.error('body:'..body)
    return ret
end
local function sort_params(params)
    local p = {}
    for k, v in pairs(params) do
        if k ~= 'sign' and v then
            table.insert(p,k)
        end
    end
    table.sort(p)
    local str = p[1]..'='..params[p[1]]
    p[1] = nil
    for _, k in pairs(p) do
        str = str..'&'..k..'='..params[k]
    end
    return str 
end
local function getUserInfo(openid, token)
    local ec = const.error_code
    local ret = {
        errorCode = ec.ok,
        nickname = openid,
        small_pic = nil,
        gender = nil,
    }
    local header = {
        ["content-type"] = "application/x-www-form-urlencoced"
    }
    local recvheader = {}
    local form = {
        uid = openid,
        token = token,
    }
    local content = ""--json.encode(form)
    local host = LK_GAME_HOST 
    local uri = string.format("/User/GetUserInfo?uid=%s&token=%s",openid,token)
    -- perform http request
    local ok, status, body = pcall(httpc.request,"POST", host, uri, recvheader, header, content)
    if not ok then 
        skynet.error('http request failed')
        ret.errorCode = ec.http_req_fail
        return ret
    end
    if status ~= 200 then
        skynet.error('http request failed status:'..status)
        ret.errorCode = ec.http_req_fail
        return ret
    end
    if not body or body == '' then
        skynet.error('http data error')
        ret.errorCode = ec.http_data_error
        return ret
    end
    local ok,info = pcall(json.decode(body))
    if not ok then 
        ret.errorCode = ec.http_data_error
        return ret
    end
    ret.nickname = info.nickname
    ret.gender = info.gender or ret.gender
    ret.small_pic = info.image_url

    return ret
end
local function alipay_auth()
	local alipay = {
		appId = "2019052465364222",
		priKey = [[
-----BEGIN RSA PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCab0bW1tJ5ARhHlAEVcVhCpupaDjkMusfOQq03mITYnO8bFZ8xObQbGuYzjiNdSYXyCyGeFavDZgb5Ni8BrCW8IMmVE8nvsiQxpImTlodSXl9GA1rCSTUR/wG/BCnquJajyq9eCO6Ls4sMeqTlvikbSXohKms5pyAX8IxQ+QVPtFNivesQkEJa+KHowQG4fjuBkE7zIJ0TJeLSDePgxwtBqfXOdS8tjTdtRl53vskUkD99/yDVmlpqNOe+cUwkq8/jxJeBVfAptZUN/EYpbSkFZAtu19AIbCVeOKFmncBLYpX1aIrnp3+qRg15c5Yx1kz3FBhxuiJzZvQlORMshRERAgMBAAECggEAPEmUbeoO4A39XHP6uFQ3EE34zo8e1klUFsEQvdNciPM6pGgcbSsJlEs78oRaaeB52bNevEfZdb4umrr8PrrlQDvMddXzzw7L8lU5H9Rzn0QYDUzh8VaC0TOritYlRuwoYmiNbTgvZ3n/ni+Cj+1cPrrAYpIcqbUEYlQVcsbDvAkdi7BaJ4w9S8nY8IEJi4rLb1hlFL9y6hmESe6coLY1PtKxs2I852U9b67NAM2Gj68jBgApWb7554wIahobOXypsVm+82ibRS2+kVM2EpEgVmXN4fHXtxtoMEuX7J9QCh7NgFLTRKv9NKhximqg5oc0t3vPqvAp54qtC43Xux0gAQKBgQDXEWV0tDFQ/Jy/8ZusehKYFIXQ7ExEnY0DWpFKYHV6aRXc7QhWzIq6jTuZLHl5blAdh7TUwdWxfkRvDhcQS7/TOx1+Cenhqwqpujh6xR7d/194zoSfuZ+Nxiim2ygR5H8S1xiG4xssF15RnKroCDQLR6QcXQ1MTgP5Cw/kmPdu8QKBgQC307AjgpIgW5DUF93T6s4HtQw1h6opnVZykG7s646qF2sbOKPqsa2/G3oSyVMBjGraL5L9NXdLhJ2KPQ4iCQhz9AaEHyGfkU8k+PhktbmH8kOb/qBrnYmEbMwskul3jaT/NGcxuzrRFfh7eT1wrDHA+lvmj1BADiBVVhblzSQEIQKBgGyOY1+c9KekWpqoIaPVmj0ZXtIt8/FOdTcUJ/Ia7jFziY2SN3tYk03GrCaMxGP+woGlViluDachzPkha5LWKWU9mY1dA6jc5Oz3Cjst9H6pwE8CLv9QerDdLPfL91XQuDOSTtcHG1gOE5WuNrYYLezjpHqMnYG+f1Lduo/whjSBAoGAS06Nef6jnl3klv6wAYDPppikx8ksDKBIKebBio11hHjTh/NMM3lWlFPNTqTrPFcp7vhOXyzoEifh4h/hClBpBheSE84tGBGPOrZEC/3mY6r7x4woa+29yGJJwnjs6+07CtGM9PKexckaYnrokJ/6QfROccWEOrmOxWbfOoUvnAECgYEAyelEGr5Euz7B3HJ5TL3aLz49QVcMd5E/Z7bWY4i6YlbzSCfnw6AV5pI3kjdsADNkJLY+zRE/oSUFmEDes4WQYw0mcC3eJ10oINtOrycl3P6mnWSbMBuMZ1+VSaBJIrL8M1bQ8fRuTAKYfBS88WA9UwLINK0on6MbtjuUUkX91bc=
-----END RSA PRIVATE KEY-----]]
	}
	
	if not alipay and next(alipay) then
		logger.error("get alipay appid failed")
		return code_utils.package(cmd, code_constants.FAILURE)
	end
	if true then
		local biz_content = {
			scopes = "auth_user"
		}

		local params = {}
		params.app_id = alipay.appId
		params.method = "alipay.user.info.auth"
		params.format = "JSON"
		params.charset = "utf8"
		params.sign_type = "RSA2"
		params.sign = nil
		params.timestamp = futil.nowstr() 
		params.version = "1.0"
		params.biz_content = json.encode(biz_content)
		local sign_str = sort_params(params)
		local sign = codec.rsa_private_sign_sha256withrsa(sign_str, alipay.priKey)
		params.sign = codec.base64_encode(sign)
		logger.debug("params:%s", table.tostring(params))
		local host = "https://openapi.alipay.com/gateway.do"
		--[[
		local ok, res = skynet.call(constants.unique_services.ssl.name, "lua","request", 
		host, cjson_encode(params))
		]]
		local ok, res = skynet.call('.webclient', 'lua', 'request', host, nil, params, false)
		if not ok then
			logger.error("https request failed:%s", tostring(res))
		else
			logger.debug("https request success:%s", res)
		end
	end
end
local function generate_order_id()
    local t = skynet.time()
    return string.format("%s",math.floor(t*100))
end


local function handle_weixin_native_order(openid, uid, diamond_info, price_config, args)
    local ec = {
        ok = 0,
    }
    local pay_gateway = "https://api.mch.weixin.qq.com/pay/unifiedorder"
    local notify_url = "http://112.74.198.84:8007/payment_notify/weixin_native"
    local wcpay_config = {
        appid = "",
        mch_id = "",
        key = "",
    }
    local content = {
        name = "xml",
    }
    local appid = {
        name = "appid",
        text = wcpay_config.appid
    }
    local mch_id = {
        name = "mch_id",
        text = wcpay_config.mch_id
    }
    local nonce_str = {
        name = "nonce_str",
        text = math.random(1000,10000)
    }
    local sign = {
        name = "sign",
    }
    local sign_type = {
        name = "sign_type",
        text = "MD5",
    }
    local body = {
        name = "body",
        text = args.body
    }
    local out_trade_no = {
        name = "out_trade_no",
        text = generate_order_id() 
    }
    local total_fee = {
        name = "total_fee",
        text = (price_config.price - price_config.discountable)*100,
    }
    local spbill_create_ip = {
        name = "spbill_create_ip",
        text = "127.0.0.1",
    }
    local t = os.time()
    local expire = t+2*60*60
    local time_start = {
        name = "time_start",
        text = os.date("%Y%m%d%H%M%S",t) 
    }
    local time_expire = {
        name = "time_expire",
        text = os.date("%Y%m%d%H%M%S",expire) 
    }
    local notify_url = {
        name = "notify_url",
        text = notify_url,
    }
    local product_id = {
        name = "product_id",
        text = "x"
    }
    local trade_type = {
        name = "trade_type",
        text = "JSAPI",
    } 
    local openid = {
        name = "openid",
        text = "ogQy10tKNFRrYHFpGYJWglvxN4mI"
    }
    local kids = {appid, mch_id, nonce_str,sign,sign_type,body,out_trade_no,
    total_fee,spbill_create_ip,time_start,time_expire,notify_url,product_id,
    trade_type,openid
    }
    local kv = {}
    for _, p in pairs(kids) do
        kv[p.name] = p.text
    end
    local sign_str = sort_params(kv).."&key="..wcpay_config.key
    logger.warn(string.format('sign str:%s', sign_str))
    local signacture = string.upper(md5.sumhexa(sign_str))
    sign.text = signacture
    content.kids = kids
    local xml = xmlser.serialize(content)
    logger.warn('xml:%s', xml)
    local ret = {
        error_code = ec.ok,
        err_desc = "OK",
        order_id = out_trade_no.text,
        charge_params = xml,
        pay_gateway = pay_gateway,
    }
    return ret 
end
local function test_weixin_pay()
    local price_config = {
        price = 1,
        discountable = 0,
    }
    local diamond_info = {}
    local args = {
        charge_value = 1,
        charge_plat = 'wcpay',
        body = "内容",
        subject = "标题",
    }
    local ret = handle_weixin_native_order('openid', 'uid', diamond_info, price_config, args)
    local header = {
        ["content-type"] = "application/x-www-form-urlencoced"
    }
    local recvheader = {}
    logger.info('order ret:%s',futil.toStr(ret))
    local content = "<xml><appid>wx15f022196ed61ff2</appid><mch_id>1484306952</mch_id><nonce_str>1414</nonce_str><sign>C512F2BDB5B7014A885BA3399737CC78</sign><sign_type>MD5</sign_type><body>公众号充值</body><out_trade_no>bypc201708101128529366775443</out_trade_no><total_fee>100</total_fee><spbill_create_ip>0.0.0.0</spbill_create_ip><time_start>20170810112852</time_start><time_expire>20170810132852</time_expire><notify_url>http://112.74.198.84:8007/payment_notify/wcpay_native</notify_url><product_id>0</product_id><trade_type>JSAPI</trade_type><openid>ogQy10tKNFRrYHFpGYJWglvxN4mI</openid></xml>" 
    local host = ret.pay_gateway 
    -- perform http request
    local ok,body= skynet.call('.webclient', 'lua', 'request', host, nil, content, false)
    if ok then
        logger.warn("success:%s", body)
    else
        logger.err('request failed,host:%s,%s',host,body)
    end
end
local function get_swiftpass_order(openid, uid, args)
    skynet.error('get_swiftpass_order:')
    local priv_key = "4b2ab8f6cdc60e6e80786de1f8b8ac3a"
    local content = {
        name = "xml",
    }
    local service = {}
    service.name = "service"
    service.text = "pay.weixin.native"
    local version = {}
    version.name = "version"
    version.text = "2.0"
    local charset = {}
    charset.name = "charset"
    charset.text = "UTF-8"
    local sign_type = {}
    sign_type.name = "sign_type"
    sign_type.text = "MD5"
    local sign = {}
    sign.name = "sign"
    sign.text = ""
    local mch_id = {}
    mch_id.name = "mch_id"
    mch_id.text = "102510849275"
    local sub_mch_id = {
        name = "sub_mch_id",
        text = "102540931408"
    }
    local groupno = {
        name = "groupno",
        text = "102540931408"
    }
    local out_trade_no = {}
    out_trade_no.name = "out_trade_no"
    out_trade_no.text = generate_order_id()
    local body = {}
    body.name = "body"
    body.text = "商口描述"
    local total_fee = {}
    total_fee.name = "total_fee"
    total_fee.text = 10 
    local mch_create_ip = {}
    mch_create_ip.name = "mch_create_ip"
    mch_create_ip.text = "ipxxxxx"
    local t = os.time()
    local expire = t + 60*60*2
    --<time_start>20170605195505</time_start><time_expire>20170605202505</time_expire>
    local time_start = {
        name = "time_start",
        text = os.date("%Y%m%d%H%M%S",t),
    }
    local time_expire = {
        name = "time_expire",
        text = os.date("%Y%m%d%H%M%S",expire),
    }
    local notify_url = {}
    notify_url.name = "notify_url"
    notify_url.text = "106.75.148.223:8003/payment_notify/swiftpass"
    local nonce_str = {}
    nonce_str.name = "nonce_str"
    nonce_str.text = "random_str"

    local kids = {service, version, charset, sign_type, sign, mch_id, out_trade_no, body, total_fee,
    mch_create_ip, notify_url, nonce_str,time_start, time_expire}
    local kv = {}
    for _, p in pairs(kids) do
        kv[p.name] = p.text
    end
    local sign_str = sort_params(kv).."&key="..priv_key
    skynet.error(string.format('sign str:%s', sign_str))
    local signacture = string.upper(md5.sumhexa(sign_str))
    sign.text = signacture
    content.kids = kids

    skynet.error('xxxxxxxxxxxxxxxxxxxx')
    local xml = xmlser.serialize(content)
    skynet.error(string.format('xml:%s', xml))

    -- get qr code
    local header = {
        ["content-type"] = "application/x-www-form-urlencoced"
    }
    local recvheader = {}
    local form = {
        uid = openid,
        token = token,
    }
    local content = xml 
    local host = "https://pay.swiftpass.cn/pay/gateway" 
    local uri = string.format("/pay/gateway")
    -- perform http request
    local ok,body= skynet.call('.webclient', 'lua', 'request', host, nil, content, false)

    --[[
    local ok, status, body = pcall(httpc.request,"POST", host, uri, recvheader, header, content)
    if not ok then
        skynet.error('http request no ok')
        return
    end
    if status ~= 200 then
        skynet.error(string.format('status code:%s ~= 200', status))
        return
    end
    ]]
    skynet.error('ALL OK:%s', body)
end
local function test_xml()
end
local function test_http_post()
    local host = "http://112.74.198.84:8080/agent/register?account=a&password=a&memberid=1" 
    local ok, info = skynet.call('.webclient', 'lua', 'request', host, nil, false)
    if not ok then
        logger.err('test_http_post failed')
        return
    end
    logger.info('test http post success:%s', info)
end

local function wechat_transfer()
	local appid = "wx9c7d915d8823db23"
	local partnerKey = "edg8p9BE1uolapAlpc4o79dlvN3Feny8"
	local mchId = "1508131681"--新版天和微信商户
	local appSecret = "ac3333d6ec1d6437867459633fb62921"
	local pub_key = ""
    local pri_key = 
[[
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAwZesbRwWydkPJV9KjDa+OJi5KKBgX8c63GxQORh33mZS+TT/
brp++IVhB0mIqSDtZFoagigXWTe5maQEsJAAivseVwRopWq+ZtmNt6CpPksAsv5w
F7MYKmAZuj8TKaNmhwnzkExZPDIPGJgov+njsfD1CRwiT1JOcy1zd4T1Bdo4UPjb
zYIBjchUPSO7MzFjRmCEsZ3I8k9M7jrEJtsbVv2IJ6vd6tASqNoT3zxmk5RxUE7v
jjgQ9nm2IYS9KQtikkj39YkqnqVdVcfWIJ1HNh23/44SXUvS/nL++kChMAUbuXeP
C9gcdS+0fJE8qHlCVWUS21vWtNZrHI9lAEl7XwIDAQABAoIBAF/vZt4nJk/exfey
MkIrurZXUKKGX1v3Yf7rmhHBQ12t/X5Lui1INDW5+yxeT1/o1lt9n1dSwMdQqyQt
OLm6ktpMuWtL3wPiUvqq4uTVtCkPiAgruKa19MrDFtzJ9xgSRnOzBcVDYJFJCVwZ
w0/fexuqGfPqwkHmusOvCWJ4O+gqrx/WTt6ajCUr7oSb0Y46ELYDJYAVsuv/G7b3
ExDtyQ88I+mXJy1QNkcaQ/l/EhIDhfEospUK6M0rzc3IlmSIY9KO6u6PcxAJ2ox2
2KYh+PC4M7UOvnP/7FNhBA7EqajDBLPP/GQZursBm18+0IJ7JJKL2wOa3EGXnIrv
atFkLQECgYEA9r/AyPnZcdUViXNBKF9fj9WAy4YzNMPqo0GUYHPNSpfjHUfJwPTa
k06U2TO4sttFr99lyAvmIOexzK6/jReTHT0w3FmuBpaDBKC3nJxy7iLpHASH6aZb
mDgVhclIsiVw4JHylpaucW2n4TlKJxIz0DdNt3Dv1C313zGTiff7/4ECgYEAyNm8
EX7O0zazGBiOwfLbJL+Q6PeebAe4r0U5rn8X++Mj8j4sWC+r7Oi1+NROWUqODW5N
8y3e9vxe2KNg6b9cCGy6W34iz7l9uUX22569hYTct06hVHbp68gs5+cYjFVSzXqE
SHzvGDRC162cdCiu//s7P+TuyBsn6eWLkkE66t8CgYB8t30U2BxNCfvhxmyHoHUn
uS1pMYKOR/w/2jTJ754y9sRnl1Jlgh08WXqoshjH5ka51zuVulXuCc33e9f704+b
NsOMjJOGZusAGs/Ti8wXi3OxoqSjt18SeD6AqbVhvcTo7TvlW3H+iQNStmdBilTA
CEPy1VWTNEvTLTa6hKpNgQKBgFfkaFdjnZByJGdL/9TBuMJZDknUakAuFNSmP3qr
5Uv19vn/2RnyKpMutssf5PVQGd+owHXFQgflIoA85qEDe3u4UMjO5t7t9iWIh2FO
EvOF06xnvVOgAfeLDpOg3m4yvFxs28x414xI+mM1dvyh/QrJ3wCz5wYsVAgXyj8D
SowTAoGBAMIIULP3lpxzga+CkneuWxeh9L5ZD2hEdhnLvQFPP30o4wezW0dAUGNa
zX+7zfFpxp4nwtioYIKzBXAACm8eedEENPDRL4vgvVaW13E8YM2tvGk5BWowPlC5
flYUaiUILjveZDOawmy6Pvsj3vkzyaztJHHK/kwB0kH6qWGM+Aq0
-----END RSA PRIVATE KEY-----]]
	local params = {}
	params.mch_appid = appid
	params.mchid = mchId
	params.nonce_str = tostring(math.random(1000, 9999))
	params.sign = nil
	params.partner_trade_no = "6537989100111134724"--generate_order_id()
	params.openid = "oh4OT5yVp1QN_Ipi-tZmPAMZxVwo"
	params.check_name = "NO_CHECK"
	params.amount = 1 * 100 --分
	params.desc = "提现测试"
	params.spbill_create_ip = "127.0.0.1"
    local sign_str = (sort_params(params))
	--[[
	logger.debug("begin rsa:%s", sign_str)
	local ok, bs = xpcall(codec.rsa_private_sign_sha256withrsa, handle_error, sign_str, pri_key)
	if not ok then
		logger.err("call failed:%s", tostring(bs))
	end
	]]
	sign_str = sign_str.."&key="..partnerKey
	logger.debug("sign_str:%s", sign_str)
	params.sign = md5.sumhexa(sign_str) 
	local url = "https://api.mch.weixin.qq.com/mmpaymkttransfers/promotion/transfers"
    --local content = xmlser.serialize(params)
    local content = string.format([[
	<xml>
	<mch_appid>%s</mch_appid>
	<mchid>%s</mchid>
	<nonce_str>%s</nonce_str>
	<partner_trade_no>%s</partner_trade_no>
	<openid>%s</openid>
	<check_name>%s</check_name>
	<amount>%s</amount>
	<desc>%s</desc>
	<spbill_create_ip>%s</spbill_create_ip>
	<sign>%s</sign>
	</xml>
	]],params.mch_appid,params.mchid, params.nonce_str,params.partner_trade_no,
	params.openid,params.check_name, params.amount, params.desc, params.spbill_create_ip,
	params.sign) 
	logger.debug("send https:%s", content)
	local cert = "/home/qyh/client_key/apiclient_cert.pem"
	local sslkey = "/home/qyh/client_key/apiclient_key.pem"
    local ok,body= skynet.call('.webclient', 'lua', 'pem_request', url, nil, content, "cainfo", cert, sslkey)
	if not ok then
		logger.err("https request failed:%s", body)
		return false
	end
	logger.debug("https request success:%s", body)
	return true
end
local function alipay_app()
    local pri_key = 
[[
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAwZesbRwWydkPJV9KjDa+OJi5KKBgX8c63GxQORh33mZS+TT/
brp++IVhB0mIqSDtZFoagigXWTe5maQEsJAAivseVwRopWq+ZtmNt6CpPksAsv5w
F7MYKmAZuj8TKaNmhwnzkExZPDIPGJgov+njsfD1CRwiT1JOcy1zd4T1Bdo4UPjb
zYIBjchUPSO7MzFjRmCEsZ3I8k9M7jrEJtsbVv2IJ6vd6tASqNoT3zxmk5RxUE7v
jjgQ9nm2IYS9KQtikkj39YkqnqVdVcfWIJ1HNh23/44SXUvS/nL++kChMAUbuXeP
C9gcdS+0fJE8qHlCVWUS21vWtNZrHI9lAEl7XwIDAQABAoIBAF/vZt4nJk/exfey
MkIrurZXUKKGX1v3Yf7rmhHBQ12t/X5Lui1INDW5+yxeT1/o1lt9n1dSwMdQqyQt
OLm6ktpMuWtL3wPiUvqq4uTVtCkPiAgruKa19MrDFtzJ9xgSRnOzBcVDYJFJCVwZ
w0/fexuqGfPqwkHmusOvCWJ4O+gqrx/WTt6ajCUr7oSb0Y46ELYDJYAVsuv/G7b3
ExDtyQ88I+mXJy1QNkcaQ/l/EhIDhfEospUK6M0rzc3IlmSIY9KO6u6PcxAJ2ox2
2KYh+PC4M7UOvnP/7FNhBA7EqajDBLPP/GQZursBm18+0IJ7JJKL2wOa3EGXnIrv
atFkLQECgYEA9r/AyPnZcdUViXNBKF9fj9WAy4YzNMPqo0GUYHPNSpfjHUfJwPTa
k06U2TO4sttFr99lyAvmIOexzK6/jReTHT0w3FmuBpaDBKC3nJxy7iLpHASH6aZb
mDgVhclIsiVw4JHylpaucW2n4TlKJxIz0DdNt3Dv1C313zGTiff7/4ECgYEAyNm8
EX7O0zazGBiOwfLbJL+Q6PeebAe4r0U5rn8X++Mj8j4sWC+r7Oi1+NROWUqODW5N
8y3e9vxe2KNg6b9cCGy6W34iz7l9uUX22569hYTct06hVHbp68gs5+cYjFVSzXqE
SHzvGDRC162cdCiu//s7P+TuyBsn6eWLkkE66t8CgYB8t30U2BxNCfvhxmyHoHUn
uS1pMYKOR/w/2jTJ754y9sRnl1Jlgh08WXqoshjH5ka51zuVulXuCc33e9f704+b
NsOMjJOGZusAGs/Ti8wXi3OxoqSjt18SeD6AqbVhvcTo7TvlW3H+iQNStmdBilTA
CEPy1VWTNEvTLTa6hKpNgQKBgFfkaFdjnZByJGdL/9TBuMJZDknUakAuFNSmP3qr
5Uv19vn/2RnyKpMutssf5PVQGd+owHXFQgflIoA85qEDe3u4UMjO5t7t9iWIh2FO
EvOF06xnvVOgAfeLDpOg3m4yvFxs28x414xI+mM1dvyh/QrJ3wCz5wYsVAgXyj8D
SowTAoGBAMIIULP3lpxzga+CkneuWxeh9L5ZD2hEdhnLvQFPP30o4wezW0dAUGNa
zX+7zfFpxp4nwtioYIKzBXAACm8eedEENPDRL4vgvVaW13E8YM2tvGk5BWowPlC5
flYUaiUILjveZDOawmy6Pvsj3vkzyaztJHHK/kwB0kH6qWGM+Aq0
-----END RSA PRIVATE KEY-----]]
    local pub_key = 
[[
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwZesbRwWyd
kPJV9KjDa+OJi5KKBgX8c63GxQORh33mZS+TT/brp++IVhB0mIqSDt
ZFoagigXWTe5maQEsJAAivseVwRopWq+ZtmNt6CpPksAsv5wF7MYKm
AZuj8TKaNmhwnzkExZPDIPGJgov+njsfD1CRwiT1JOcy1zd4T1Bdo4
UPjbzYIBjchUPSO7MzFjRmCEsZ3I8k9M7jrEJtsbVv2IJ6vd6tASqN
oT3zxmk5RxUE7vjjgQ9nm2IYS9KQtikkj39YkqnqVdVcfWIJ1HNh23
/44SXUvS/nL++kChMAUbuXePC9gcdS+0fJE8qHlCVWUS21vWtNZrHI
9lAEl7XwIDAQAB
-----END PUBLIC KEY-----
]]
    skynet.error('alipay_app_order:')
    local biz_content = {}
    biz_content.out_trade_no = generate_order_id()
    biz_content.subject = ("title")
    biz_content.total_amount = 2
    biz_content.body = "body"
    biz_content.product_code = "QUICK_MSECURITY_PAY"
   
    local params = {}
    params.app_id = "2019022063257325" 
    params.method = "alipay.trade.app.pay"
    --params.method = "alipay.trade.precreate"
    params.charset = "utf-8"
    --params.format = "json"
    params.sign_type = "RSA2"
    params.version = "1.0"
    params.timestamp = os.date("%Y-%m-%d %H:%M:%S", os.time())
    --params.notify_url = "http://dev1_game.happyfish.lkgame.com:8007/payment_notify/alipay_trade_precreate"
    params.notify_url = "http://dev1_game.happyfish.lkgame.com:8080/getPost"
    params.biz_content = json.encode(biz_content)
    skynet.error('alipay_app:')
    local sign_str = (sort_params(params))
    skynet.error(string.format('sign string:%s', sign_str))
	--[[
    local priv_key, priv_type = cryptopp.pem2der(pri_key)
    local signer = cryptopp.rsa_signer(priv_key)
    local sign = (cryptopp.base64enc(signer(sign_str)))
	]]
	logger.debug("%s", codec.rsa_private_sign_sha256withrsa)
	local ok, bs = xpcall(codec.rsa_private_sign_sha256withrsa, handle_error, sign_str, pri_key)
	if not ok then
		logger.err("call failed:%s", tostring(bs))
	end
	local sign = codec.base64_encode(bs)
    skynet.error(string.format('sign:%s', sign))
    params.sign = sign 
    --- send http request
    local header = {
        --["content-type"] = "application/x-www-form-urlencoced"
        ["content-type"] = "application/text"
    }
    local recvheader = {}
    local form = {
        app_id = params.app_id,
        sign = params.sign,
    }
    --验签 
    logger.debug("begin verifier")
	local dbs = codec.base64_decode(sign)
	local ok = codec.rsa_public_verify_sha256withrsa(sign_str, dbs, pub_key, 2)
    logger.debug("verifier done")
    logger.debug("verifier:%s", ok)
    local content = json.encode(params)
    --local host = "https://openapi.alipaydev.com/gateway.do" 
    local host = "https://openapi.alipay.com/gateway.do" 
    local uri = string.format("/")
    -- perform http request
    skynet.error(string.format('send http request:..'))
    local r,info = skynet.call('.webclient', 'lua', 'request', host, nil, params, false)
    if not r then
        skynet.error('request failed')
        return
    end
    skynet.error(string.format('success:%s', info))
    local file2=io.output("resp.html") 
    io.write(info)
    io.flush()
    io.close()
end


local function alipay_precreate()
    local pri_key = "xxxxxx"
    skynet.error('alipay_precreate:')
    local biz_content = {}
    biz_content.out_trade_no = generate_order_id()
    biz_content.subject = ("支付标题")
    biz_content.total_amount = 2
    biz_content.body = "body"
   
    local params = {}
    params.app_id = alipay_config.app_id
    params.method = "alipay.trade.precreate"
    params.charset = "utf-8"
    --params.format = "JSON"
    params.sign_type = "RSA"
    params.version = "1.0"
    params.timestamp = os.date("%Y-%m-%d %H:%M:%S", os.time())
    --params.notify_url = "http://dev1_game.happyfish.lkgame.com:8007/payment_notify/alipay_trade_precreate"
    params.notify_url = "http://dev1_game.happyfish.lkgame.com:8080/getPost"
    params.biz_content = json.encode(biz_content)
    skynet.error('alipay_precreate:')
    local sign_str = (sort_params(params))
    skynet.error(string.format('sign string:%s', sign_str))
    local priv_key, priv_type = cryptopp.pem2der(alipay_config.rsa_pri)
    local signer = cryptopp.rsa_signer(priv_key)
    local sign = (cryptopp.base64enc(signer(sign_str)))
    skynet.error(string.format('sign:%s', sign))
    params.sign = sign 
    --- send http request
    local header = {
        --["content-type"] = "application/x-www-form-urlencoced"
        ["content-type"] = "application/text"
    }
    local recvheader = {}
    local form = {
        app_id = params.app_id,
        sign = params.sign,
    }
    local content = json.encode(params)
    local host = "https://openapi.alipaydev.com/gateway.do" 
    local uri = string.format("/")
    -- perform http request
    skynet.error(string.format('send http request:..'))
    local r,info = skynet.call('.webclient', 'lua', 'request', host, nil, params, false)
    if not r then
        skynet.error('request failed')
        return
    end
    --[[
    local ok, status, body = pcall(httpc.request,"POST", host, uri, recvheader, header, content)
    if not ok then 
        skynet.error('http request failed')
        ret.errorCode = ec.http_req_fail
        return ret
    end
    if status ~= 200 then
        skynet.error('http request failed status:'..status)
        ret.errorCode = ec.http_req_fail
        return ret
    end
    if not body or body == '' then
        skynet.error('http data error')
        ret.errorCode = ec.http_data_error
        return ret
    end
    local ok,info = pcall(json.decode(body))
    if not ok then 
        ret.errorCode = ec.http_data_error
        return ret
    end
    ]]
    skynet.error(string.format('success:%s', info))

end

local function test_rsa()
    skynet.error('test_rsa:')
    local sign_str = 'a=123'
    local priv_key, priv_type = cryptopp.pem2der(alipay_config.rsa_pri)
    local signer = cryptopp.rsa_signer(priv_key)
    local sign = signer(sign_str)
    local sign_base64 = cryptopp.base64enc(sign)
    --local sign = codec.rsa_private_sign(sign_str, alipay_config.rsa_pri)
    --local sign_base64 = codec.base64_encode(sign)

    --skynet.error(string.format('sign:%s', sign))
    skynet.error(string.format('sign_base64:%s', sign_base64))
    local pub_key, pub_type = cryptopp.pem2der(alipay_config.rsa_pub)
    local verifier = cryptopp.rsa_verifier(pub_key)
    local verify_str = 'app_id=2016080200153059&auth_app_id=2016080200153059&body=body&buyer_id=2088102172197965&buyer_logon_id=odr***@sandbox.com&buyer_pay_amount=2.00&charset=GBK&fund_bill_list=[{"amount":"2.00","fundChannel":"ALIPAYACCOUNT"}]&gmt_create=2017-05-15 15:17:23&gmt_payment=2017-05-15 15:17:47&invoice_amount=2.00&notify_id=bbb6fe8b93fb2417cab961189ea4603neq&notify_time=2017-05-15 15:17:47&notify_type=trade_status_sync&open_id=20881064086152293543565750017196&out_trade_no=1494832554.78&point_amount=0.00&receipt_amount=2.00&seller_email=widvej2444@sandbox.com&seller_id=2088102169679711&subject=支付标题&total_amount=2.00&trade_no=2017051521001004960200272651&trade_status=TRADE_SUCCESS&version=1.0' 
    local alipay_sign = "Tsc974Di5KNWekfO8H4CbMwO/u+bpRMCT0tqBYtjKht6w9O08DVxm40wGgA6xmyAjqJVxpcVjiTJINi4O5yRxGmHhJRe54KUQqCyCUlMXsQtqBIg2kqtsYajYWganLdzEOAG4YVLaxUinZ41CzcPV+N8fMnwP9OhFm4rp/gN4L8="
    local r = verifier(verify_str, futil.urldecode(cryptopp.base64dec(alipay_sign)))
    skynet.error('verifier:%s', r)
end

local function main()
	local function f()
	end
    -- get lk openid test
    skynet.error('hello:'..md5.sumhexa('hello'))
    --[[
    local rst = getLkUserToken()
    local openid = "25811562"
    local token = rst.token 
    if validateLkUser(openid, token) then
    end
    ]]
    --test_http_post()
    --alipay_app()
    --test_rsa()
    --test_xml()
    --get_swiftpass_order(nil, nil, {})
    --test_weixin_pay()
	--wechat_transfer()
	alipay_auth()
    logger.debug("debug log test")    
    logger.info("info log test")    
    logger.warn("warn log test")    
    logger.err("error log test")    
end
skynet.start(function () 
    local ok, r = xpcall(main, handle_error)
	if not ok then
		logger.debug("%s", tostring(r))
	end
    skynet.exit()
end)


