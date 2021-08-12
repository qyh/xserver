thread = 8
name = "login"
logger = "../log/"..name.."_core"..".log" 
logpath = "../log"
harbor = 0
start = "main" 
bootstrap = "snlua bootstrap"    -- The service for bootstrap
luaservice = "./service/?.lua;".."../src/app/"..name.."/?.lua;"
	.."../src/service/?.lua;"
    .."../src/service/cluster/?.lua;"
    .."./lualib/compat10/?.lua;"
    .."../src/service/login/?.lua;"
lualoader = "lualib/loader.lua"
cpath = "./cservice/?.so;../luaclib/?.so;"
preload = '../src/app/'..name..'/preload.lua'
redis = "test" 
db = "test"
--daemon="../run/"..name..".pid"

--cluster config
nodename = "login1"
nodetype = 2 
nodeip = "127.0.0.1"
nodeport = "50607"
nodeid = 3 
--cluster config end
proto = "x"
