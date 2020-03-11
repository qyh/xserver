local conf = {}


conf.mysql = {
    localhost = {
        host = "192.168.82.101",
        port = "3306",
        database = "Zipai",
        user = "root",
        password = "123",
    },
    ["2017_118"] = {
        host = "47.112.252.58",
        port = "3306",
        database = "Zipai",
        user = "qyh_2019_db_user",
        password = "qS6.WaQr0nZv0*&G",
    },
    ["2017_115"] = {
        host = "47.115.47.54",
        port = "3306",
        database = "GameLog",
        user = "qyh_2019_db_user",
        password = "qS6.WaQr0nZv0*&G",
    },
    ["2018_118"] = {
        host = "8.129.15.148",
        port = "3306",
        database = "Zipai",
        user = "qyh_2019_db_user",
        password = "qS6.WaQr0nZv0*&G",
    },
    ["2018_115"] = {
        host = "47.115.124.181",
        port = "3306",
        database = "GameLog",
        user = "qyh_2019_db_user",
        password = "qS6.WaQr0nZv0*&G",
    },
    ["2019_118"] = {
        host = "120.79.9.214",
        port = "3306",
        database = "Zipai",
        user = "qyh_2019_db_user",
        password = "qS6.WaQr0nZv0*&G",
    },
    ["2019_115"] = {
        host = "120.77.178.144",
        port = "3306",
        database = "GameLog",
        user = "qyh_2019_db_user",
        password = "qS6.WaQr0nZv0*&G",
    },
}
conf.mysqldb_name = {}
for k,v in pairs(conf.mysql) do
    conf.mysqldb_name[k] = k
end

return conf
