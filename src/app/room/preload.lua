-- This file will execute before every lua service start
-- See config
local skynet = require "skynet"
local name = skynet.getenv("name")
package.path = './service/?.lua;'
..'../src/app/'..name..'/?.lua;'
..'../src/common/?.lua;'
..'../src/service/?.lua;'
..'../src/service/room/?.lua;'
..'../src/service/cluster/?.lua;'
.."./lualib/compat10/?.lua;"
..package.path..';'
package.cpath = package.cpath..";./cservice/?.so;../luaclib/?.so;"

