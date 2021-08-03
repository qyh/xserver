local skynet = require "skynet"
local logger = require "logger"
local futil = require "futil"
local const = require "const"
local pb = require "pubsub"
require "tostring"
require "skynet.manager"
local CMD = {}
local timeout = 30

local wait_cos = {}
function CMD.lock(k, v, sec)
    if not (k and v) then
        return false
    end
    sec = sec or timeout
    local rv = pb:set(k, v, "ex", sec, "nx")
    if rv then
        return true
    end
    return false
end

function CMD.unlock(k, v)
    local r = pb:get(k)
    if not r then
        return false
    end
    if r and tostring(r) == tostring(v) then
        pb:del(k)
        pb.pub(const.pubsubChannel.ch_release_lock, k)
        return true
    end
    return false
end

function CMD.lock_wait(k, v, sec)
    local co = coroutine.running()
    sec = sec or timeout
    local ok = false
    local is_timeout = false
    local enter_t = os.time()
    while true do
        if not CMD.lock(k, v, sec) then
            table.insert(wait_cos, {co=co, k=k, v=v, sec=sec, enter_t=enter_t})
            skynet.wait(co)
        else
            local cur_t = os.time()
            if cur_t - enter_t > sec then
                is_timeout = true
                ok = false
            else
                is_timeout = false
                ok = true
            end
            break
        end
    end
    return ok, is_timeout    
end

function CMD.on_message(ch, msg)
    if ch == const.pubsubChannel.ch_release_lock then
        for k, v in pairs(wait_cos) do
            if v.k == msg then
                local info = table.remove(wait_cos, k)
                if info then
                    skynet.wakeup(info.co)
                else
                    logger.err("%s not found on wait_cos", k)
                end
            end
        end
    end
end

local function timeout_check()
    while true do
        local cur_t = os.time()
        for k, v in pairs(wait_cos) do
            if cur_t - v.enter_t > v.sec then
                local info = table.remove(k)
                skynet.wakeup(info.co)
            end
        end
        skynet.sleep(100)
    end
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
    local ok = pb.sub(const.pubsubChannel.ch_release_lock, "on_message")
    if not ok then
        logger.err("sub %s fail", const.pubsubChannel.ch_release_lock)
    else
        logger.info("sub %s success", const.pubsubChannel.ch_release_lock)
    end
    skynet.fork(timeout_check)
    skynet.register(".mt_lock_service")
end)

