local skynet = require "skynet"
local redis = require "skynet.db.redis"
local json = require "cjson"
local logger = require "logger"
local futil = require "futil"
local const = require "const"
local mt_lock = require "mt_lock"
local https = require "https"
local mysql_aux = require "mysql_aux"
require "tostring"
require "skynet.manager"
local CMD = {}

function CMD.audit_mt_lock(...)
    logger.debug("audit_mt_lock")
    for i=1, 10 do
        skynet.fork(function()
            local k = string.format("locktest")
            logger.debug("%i try get lock %s", i, k)
            local ok, timeout = mt_lock.lock_wait(k, i, 30)
            logger.debug("%i get lock %s success", i, k)
            mt_lock.unlock(k, i)
            logger.debug("%i unlock %s", i, k)
        end)
    end
    logger.debug("audit_mt_lock end")
end

function CMD.convertip()
    local host = 'https://ipcheck.market.alicloudapi.com/convertip'
    --local host = 'https://baidu.com'
    local get = false
    local post = {
        src = '111.194.236.48',
    } 
    local appCode = "2ba2734a68034076b7ffc50641217da5"
    local header = {
        ['Authorization'] = 'APPCODE '..appCode,
        ['Content-Type'] =  'application/x-www-form-urlencoded; charset=UTF-8',
    }
    local ok, rv = https.post(host, post, header) 
    if ok then
        local msg = json.decode(rv)
        logger.debug("rv:%s", futil.toStr(msg))
    else
        logger.err("request fail")
    end
end

function CMD.test()
    logger.debug("CMD.test")
    logger.debug("CMD.test end")
    skynet.fork(function()
        while true do
            local res = mysql_aux.localhost.exec_sql("select * from my_test")
            logger.debug("res:%s", futil.toStr(res))
            skynet.sleep(100)
        end
    end)
end

skynet.init(function()
    
end)

skynet.start(function()
    skynet.dispatch('lua', function(session, address, cmd, ...)
        local f = CMD[cmd]
        if f then
            if session > 0 then
                skynet.ret(skynet.pack(f(...)))
            else
                f(...)
            end
        else
            logger.err('ERROR: Unknown command:%s', tostring(cmd))
        end
    end)
    skynet.register(".xservice")
end)

