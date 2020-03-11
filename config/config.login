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
	"port": 6379
}]]
mysql_conf = [[{
	"host":"192.168.83.178",
	"port":3306,
    "user":"root",
    "password":"root",
    "database":"imserver"
}]]
websocket_port = 8008
simpleweb_port = 8080
--daemon="../"..name..".pid"
