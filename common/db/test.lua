local conf = {}

conf.redis = {
    ["pubsub"] = {
        host = "127.0.0.1",
        port = 6379,
    },
    [1] = {
        host = "127.0.0.1",
        port = 6379,
    },
}

conf.mysql = {
    localhost = {
        host = "192.168.82.101",
        port = "3306",
        database = "Zipai",
        user = "root",
        password = "123",
        type = 0,
        max_packet_size = 10 * 1024 * 1024,
    },
}
conf.mysqldb_name = {}
for k,v in pairs(conf.mysql) do
    conf.mysqldb_name[k] = k
end

return conf
