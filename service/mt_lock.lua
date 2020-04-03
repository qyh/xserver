local skynet = require "skynet"
local mt_lock = {}
local service = ".mt_lock_service"

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
