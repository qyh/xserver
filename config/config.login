thread = 8
name = "xserver"
--logger = "../log/"..name..".log" 
logpath = "../log"
harbor = 0
start = "main"
bootstrap = "snlua bootstrap"	-- The service for bootstrap
luaservice = "./service/?.lua;../server/?.lua;./examples/login/?.lua;"
	.."../service/?.lua;"
lualoader = "lualib/loader.lua"
cpath = "./cservice/?.so;../luaclib/?.so;"
preload = '../server/preload.lua'
websocket_port = 8008
simpleweb_port = 80 
--daemon="../skynet.pid"
