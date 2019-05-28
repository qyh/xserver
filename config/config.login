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
redis_conf = [[{
	"host":"127.0.0.1",
	"port": 6379,
	"auth": "",
	"db": 0
}]]
mysql_conf = [[{
	"host":"127.0.0.1",
	"port":3306
}]]
websocket_port = 8008
simpleweb_port = 8000
--daemon="../"..name..".pid"
