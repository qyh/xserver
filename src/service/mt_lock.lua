local skynet = require "skynet"
local mt_lock = {}
local service = ".mt_lock_service"


function mt_lock.new_lock(k, v, sec)
    local lock_wrap = {
        k = k,
        v = v,
        sec = sec,
        lock = function()
            return skynet.call(service, "lua", "lock_wait", k, v, sec)
        end,
        unlock = function()
            return skynet.call(service, "lua", "unlock", k, v)
        end
    }
    lock_wrap.lock()
    return setmetatable(lock_wrap, {__close = function(t, err)
        t.unlock()
    end})
end

function mt_lock.lock(k,v,s)
    return skynet.call(service, "lua", "lock", k, v, s)
end

function mt_lock.unlock(k, v)
    return skynet.call(service, "lua", "unlock", k, v, s)
end

function mt_lock.lock_wait(k, v, s)
    return skynet.call(service, "lua", "lock_wait", k, v, s)
end


return mt_lock
