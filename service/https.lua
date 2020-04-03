local skynet = require "skynet"
local logger = require "logger"
local https = {}

function https.post(url, data, header)
    return skynet.call(".webclient", "lua", "post", url, data, header)
end

return https
