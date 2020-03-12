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
local const = require "const"
local prefix = const.redis_key.audit_user 

local number_key = {
    "gameCardRecharge",     --对战卡充值量
    "goldCoinRecharge",     --金币充值量
    "rechargeCount",        --充值次数
}

local fs = {
    "userID",
    "code",
    "nickName",             --昵称
    "userName",             --账号
    "goldCoin",             --目前金币数
    "level",                --游戏等级
    "regTime",              --注册时间
    "lastLoinTime",         --最后登录时间
    "firstRechargeTime",    --首次充值时间
    "lastRechargeTime",     --最后充值时间
    "rechargeCount",        --充值次数
    "totalAmount",          --累计充值金额(元)
    "activeDay",            --活跌天数
    "winCount",             --游戏胜数
    "loseCount",            --游戏负数
    "gameCardRecharge",     --对战卡充值量
    "goldCoinRecharge",     --金币充值量
}

local user = {}

function user.get_user_info(userID)
    local rds_key = string.format("%s:%s", prefix, userID)
    local rs = redis:hgetall(rkey)
    if not (rs and next(rs)) then
        return nil
    end
    local user_info = {}
    for i=1, #rs, 2 do
        if number_key[rs[i]] then
            user_info[rs[i]] = tonumber(rs[i + 1])
        else
            user_info[rs[i]] = rs[i + 1]
        end
    end
    return user_info
end

function user.set_user_info(userID, info)
    local rds_key = string.format("%s:%s", prefix, userID)
    local sets = {rds_key}
    if info and next(info) then
        for k, v in pairs(info) do
            table.insert(sets, k)
            table.insert(sets, v)
        end
        local r = redis:hmset(sets)
        return r
    end
    return nil
end

return user
