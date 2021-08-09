local skynet = require "skynet"
require "skynet.manager"

local logger = require "logger"
local redis = require "skynet.db.redis"
local futil = require "futil"
local crypt = require "crypt"
local cluster = require "lkcluster"
local skynet_util = require "skynet_util"
local dbconf = require "db.db"
redis_conf = dbconf.redis["single"]
local command = {}

--time settings
local REG_SELF_INTERVAL = 20
local UPDATE_CLUSTER_INTERVAL = 20
local UPDATE_NODEID_INTERVAL = 20
local NODE_INVALID_TIME = 60
local NODEID_EXPIRE_TIME = 60

local running = false
local nodeName = skynet.getenv("nodename")
local cluserFilePath = skynet.getenv("cluster")
local nodeFullName, nodeIp, nodePort, nodeLocalIp, nodeEndpoint, nodeid
if nodeName then
	nodeIp = assert(skynet.getenv("nodeip"), "set proper nodeip, please!")
	nodePort = assert(tonumber(skynet.getenv("nodeport")), "set proper nodeport, please!")
	nodeLocalIp = skynet.getenv("nodelocalip") or "0.0.0.0"
	nodeEndpoint = string.format("%s:%s", nodeIp, nodePort)
	nodeFullName = nodeName.."#"..os.time()
	nodeid = tonumber(skynet.getenv("nodeid"))
end
local cluster_cfg_mgr = (require "cluster_cfg_mgr").new(cluserFilePath, {})
local redisdb_single
local nodegroup = skynet.getenv("nodegroup") or "default"
--wt:with time, 原来node只注册nodename，后面把注册时间也加上了，所以就叫with time
local config_key = string.format("clusterconfig:wt:%s", nodegroup)
local redis_delta_time

local _conf = {
	redis_host = redis_conf.host,
	redis_port = redis_conf.port,
	redis_auth = redis_conf.auth,
}
assert(_conf.redis_host and _conf.redis_host ~= "")

local now_script = "return redis.call('time')[1]"
local del_script = "local r=redis.call('hget',KEYS[1],ARGV[1]) if r==ARGV[2] then redis.call('hdel',KEYS[1],ARGV[1]) else return r end"
local del_script_sha = crypt.hexencode(crypt.sha1(del_script))
local del_nodeid_script = "local r=redis.call('get',KEYS[1]) if r==ARGV[1] then redis.call('del',KEYS[1]) return 'OK' else return 'NO' end "
local del_nodeid_script_sha = crypt.hexencode(crypt.sha1(del_nodeid_script))
--update nodeid 2 redis
--如果检查到不是想要的值，仍然是要expire一次，防止是别人误设置，并且没有搞expiretime
--KEYS[1]: key
--ARGV[1]: value
--ARGV[2]: expire time
local update_nodeid_script = 
[[local r=redis.call('get', KEYS[1]) 
if (not r) then 
	redis.call('setex', KEYS[1], ARGV[2], ARGV[1])
	return {'OK'}
elseif(r==ARGV[1]) then 
	redis.call('expire', KEYS[1], ARGV[2])
	return {'OK'}
else
	local tval = redis.call('ttl', KEYS[1])
	if tval == -1 then
		redis.call('expire', KEYS[1], ARGV[2])
	end
	return {'NO', r}
end
]]
local update_nodeid_script_sha = crypt.hexencode(crypt.sha1(update_nodeid_script))


local function get_rkey_nodeid(nodegroup, nodeid)
	if not (nodegroup and nodeid) then
		error(string.format("get_rkey_nodeid invalid arg: %s, %s", nodegroup, nodeid))
	end
	return string.format("clusterconfig:nodeid:%s:%s", nodegroup, nodeid)
end

local function redis_eval_try_sha(_db, script, script_sha, num_key, ...)
	local ok, r = pcall(_db.evalsha, _db, script_sha, num_key, ...)
	if ok then return r end
	if r:find("NOSCRIPT") then
		return _db:eval(script, num_key, ...)
	end
	error(string.format("redis_eval_try_sha fail, script = %s, error = %s", script, r))
end

local function redis_now(force_adjust)
	if force_adjust or not redis_delta_time then
		-- EVAL support with twemproxy is limited to scripts that take at least 1 key
		local t = tonumber(redisdb_single:eval(now_script, 1, config_key))
		redis_delta_time = t - os.time()
		return t
	end
	return redis_delta_time + os.time()
end

local function reg_self()
	local now = redis_now()
	redisdb_single:hset(config_key, nodeFullName, string.format("%s;%s", nodeEndpoint, now))
end

local function repeat_reg_self()
	while true do
		skynet.sleep(REG_SELF_INTERVAL*100)
		if not running then
			logger.info("cluster not running, stop repeat_reg_self")
			return
		end
		xpcall(reg_self, skynet_util.handle_err)
	end
end

local function noticeExistedClusters(allConf)
	if not nodeEndpoint then 
		return 
	end

	for k,v in pairs(allConf) do
		if k ~= nodeFullName then
			local ok, err = cluster.call(k, ".clustermgr", "set", nodeFullName, nodeEndpoint)
			if not ok then
				logger.warn("clustermgr notice cluster [%s] fail", tostring(k))
			else
				logger.info("clustermgr notice cluster [%s] ok", tostring(k))
			end
		end
	end
end

local function check_expired_node(node, addr, val)
	-- check if node ok
	if cluster_cfg_mgr:get(node) == addr then
		local ok, test_r = cluster.call(node, ".clustermgr", "test")
		if ok and (test_r == node or test_r == "OK") then
			return val
		end
	end
	-- delete node
	local ok, r = pcall(redisdb_single.evalsha, redisdb_single, del_script_sha, 1, config_key, node, val)
	if not ok then
		ok, r = pcall(redisdb_single.eval, redisdb_single, del_script, 1, config_key, node, val)
		if not ok then
			logger.warn("clustermgr delete %s,%s error:%s", node, val, r)
			return val
		end
	end
	return r
end

local function del_one_node(nn)
	if not (nn and nn ~= "") then
		return
	end
	logger.info("del_one_node, %s", nn)
	redisdb_single:hdel(config_key, nn)
end

local function unregOldSelf()
	local r = redisdb_single:hgetall(config_key)
	if r and #r > 0 then
		local oldkeys = {config_key}
		for i = 1, #r, 2 do
			local name = r[i]
			if nodeName == string.match(name, "^[^#]+") then table.insert(oldkeys, name) end
		end
		if #oldkeys > 1 then redisdb_single:hdel(oldkeys) end
	end
end

local function update_cluster()
	local now = redis_now()
	local force_adjusted
	local r = redisdb_single:hgetall(config_key)
	local allConf = {}
	local expiredConf = {}
	local node_version = {}
	if nodeEndpoint then
		allConf[nodeFullName] = nodeEndpoint
	end
	if r and #r > 0 then
		local tmpConf = {}
		for i = 1, #r, 2 do
			local name = r[i]
			local val = r[i+1]
			local short_name, version = string.match(name, "^([^#]+)#?(.*)$")
			version = tonumber(version) or 0
			local oldversion = node_version[short_name]
			if short_name == nodeName then
				if name ~= nodeFullName then
					expiredConf[name] = true
				end
			elseif not oldversion or oldversion < version then
				node_version[short_name] = version
				tmpConf[name] = val
				if oldversion then
					local oldname = short_name.."#"..oldversion
					tmpConf[oldname] = nil
					expiredConf[oldname] = true
				end
			else
				expiredConf[name] = true
			end
		end
		for name, val in pairs(tmpConf) do
			local address, t = string.match(val, "([^;]*);?(.*)$")
			t = tonumber(t)
			if not t or now - t < NODE_INVALID_TIME then
				allConf[name] = address
			else
				if not force_adjusted then
					now = redis_now(true)
					force_adjusted = true
					if now - t < NODE_INVALID_TIME then
						allConf[name] = address
					end
				end
				if not allConf[name] then
					local r = check_expired_node(name, address, val)
					if r then
						if r ~= val	then
							address, t = string.match(r, "([^;]*);?(.*)$")
						end
						allConf[name] = address
					end
				end
			end
			if not allConf[name] then expiredConf[name] = true end
		end
	end
	local cacheConf = cluster_cfg_mgr:get_all()
	for name, address in pairs(cacheConf) do
		local short_name, version = string.match(name, "^([^#]+)#?(.*)$")
		if not (short_name == nodeName or allConf[name] or expiredConf[name] or node_version[short_name]) then
			local ok, test_r = cluster.call(name, ".clustermgr", "test")
			if ok and (test_r == name or test_r == "OK") then
				allConf[name] = address
			end
		end
	end
	-- filter duplicated address
	local dup_address
	if nodeEndpoint then
		dup_address = {[nodeEndpoint] = nodeFullName}
		allConf[nodeFullName] = nil
	else
		dup_address = {}
	end
	for name, address in pairs(allConf) do
		local ip, port = string.match(address, "([^:]+):(%d+)$")
		port = tonumber(port)
		if not ip or not port then
			-- invalid address
			logger.warn("clustermgr update_cluster found address [%s] of node [%s] invalid", address, name)
			allConf[name] = nil
		else
			if ip == nodeIp or ip == nodeLocalIp or ip == "127.0.0.1" or ip == "localhost" then
				ip = nodeIp or "127.0.0.1"
			end
			local dup_key = ip..":"..port
			local _name = dup_address[dup_key]
			if _name then
				logger.warn("clustermgr update_cluster found address [%s] of node [%s] duplicated with node [%s] address [%s]",
					address, name, _name, allConf[_name] or nodeEndpoint)
				-- choose nodeFullName or the newer name
				if _name == nodeFullName then
					allConf[name] = nil
				else
					local short_name, version = string.match(name, "^([^#]+)#?(.*)$")
					local _short_name, _version = string.match(_name, "^([^#]+)#?(.*)$")
					version = tonumber(version) or 0
					_version = tonumber(_version) or 0
					if version <= _version then
						allConf[name] = nil
					else
						allConf[_name] = nil
						dup_address[dup_key] = name						
					end
				end
			else
				dup_address[dup_key] = name
			end
		end
	end
	if nodeEndpoint then
		allConf[nodeFullName] = nodeEndpoint
	end
	-- reload
	if cluster_cfg_mgr:set_all(allConf) then
		cluster.reload(allConf)
	end	
	return allConf
end

local function repeat_update_cluster()
	while true do
		skynet.sleep(UPDATE_CLUSTER_INTERVAL*100)
		if not running then
			logger.info("cluster node not running, stop repeat_update_cluster")
			return
		end
		xpcall(update_cluster, skynet_util.handle_err)
	end
end

--del nodeid from redis
local function del_nodeid(nodegroup, nodeid, node_full_name)
	logger.debug("del_nodeid: %s, %s, %s", nodegroup, nodeid, node_full_name)
	local rkey = get_rkey_nodeid(nodegroup, nodeid)
	local ret = redis_eval_try_sha(redisdb_single, del_nodeid_script, del_nodeid_script_sha, 1, rkey, node_full_name)
	if ret ~= "OK" then
		logger.err("del_nodeid fail, error = %s, nodeid = %s, node_full_name = %s",
			ret, nodeid, node_full_name)
		return false
	end
	return true
end

--set nodeid 2 redis
local function set_nodeid(nodegroup, nodeid, node_full_name)
	logger.debug("set_nodeid: %s, %s, %s", nodegroup, nodeid, node_full_name)
	local rkey = get_rkey_nodeid(nodegroup, nodeid)
	local dbres = redisdb_single:set(rkey, node_full_name, 'nx', 'ex', NODEID_EXPIRE_TIME)
	if dbres ~= "OK" then
		logger.err("set_nodeid fail, error = %s, nodeid = %s, node_full_name = %s",
			dbres, nodeid, node_full_name)
		return false
	end
	return true
end

local function update_nodeid(nodegroup, nodeid, node_full_name)
	local rkey = get_rkey_nodeid(nodegroup, nodeid)
	local ret = redis_eval_try_sha(redisdb_single, update_nodeid_script, update_nodeid_script_sha, 1, rkey, 
		node_full_name, NODEID_EXPIRE_TIME)
	if not (ret and #ret > 0 and ret[1] == "OK") then
		logger.err("update_nodeid fail, ret = %s, args: %s, %s, %s",
			futil.toStr(ret), nodegroup, nodeid, node_full_name)
		return false
	end
	return true
end

--return ok, other_full_name
local function check_nodeid(nodegroup, nodeid, node_full_name)
	logger.debug("check_nodeid: %s, %s, %s", nodegroup, nodeid, node_full_name)
	if not (nodegroup and nodeid and node_full_name) then
		logger.err("check_nodeid, invalid args: %s, %s, %s", nodegroup, nodeid, node_full_name)
		return false
	end
	local rkey = get_rkey_nodeid(nodegroup, nodeid)
	local db_full_name = redisdb_single:get(rkey)
	if db_full_name then
		--is it self?
		local db_short_name = futil.short_name(db_full_name)
		local now_short_name = futil.short_name(node_full_name)
		if db_short_name ~= now_short_name then
			--check if remote is still alive, at most 3 times
			for i = 1, 3 do
				local ok, test_r = cluster.call(db_full_name, ".clustermgr", "test")
				if ok and (test_r == db_full_name or test_r == "OK") then
					logger.err("check_nodeid, same nodeid remote node is still alive! db_full_name = %s, args: %s, %s, %s", 
						db_full_name, nodegroup, nodeid, node_full_name)
					--make sure it can expire
					update_nodeid(nodegroup, nodeid, node_full_name)
					return false, db_full_name
				end
			end
		end
		--yes, we can del other node
		if not del_nodeid(nodegroup, nodeid, db_full_name) then
			logger.err("check_nodeid, del_nodeid fail, args: %s, %s, %s", nodegroup, nodeid, db_full_name)
			return false, db_full_name
		end
	end
	--set self nodeid 2 redis
	set_nodeid(nodegroup, nodeid, node_full_name)
	return true
end

local function repeat_update_nodeid()
	while true do
		skynet.sleep(UPDATE_NODEID_INTERVAL*100)
		if not running then
			logger.info("cluster not running, stop repeat_update_nodeid")
			return
		end
		xpcall(update_nodeid, skynet_util.handle_err, nodegroup, nodeid, nodeFullName)
	end
end

function command.get(name)
	if not name or name == nodeFullName then
		return nodeEndpoint
	end
	return cluster_cfg_mgr:get(name)
end

function command.set(name, address)
	if name ~= nil and address ~= nil then
		local short_name = futil.short_name(name)
		local cacheConf = cluster_cfg_mgr:get_all()
		local changed = false
		for k,v in pairs(cacheConf) do
			if k ~= name and short_name == futil.short_name(k) then
				cluster_cfg_mgr:set(k, false, true)
				cacheConf[k] = nil
				changed = true
				break
			end
		end
		changed = cluster_cfg_mgr:set(name, address) or changed
		cacheConf[name] = address
		if changed then cluster.reload(cacheConf) end
	end
end

function command.names(pat, except_self)
	return cluster_cfg_mgr:names(pat, except_self and nodeFullName)
end

function command.short_names(pat, except_self)
	return cluster_cfg_mgr:short_names(pat, except_self and nodeFullName)
end

--call when stopping server
function command.stop_cluster()
	logger.info("clustermgr stop_cluster")
	if nodeEndpoint then
		running = false
		del_one_node(nodeFullName)
	end
	return {ok = true}
end

function command.test()
	return nodeFullName
end

skynet.start(function ()
	skynet.dispatch("lua", function(session, address, cmd, ...)
		return skynet_util.lua_docmd(command, session, string.lower(cmd), ...)
	end)

	skynet.register(".clustermgr")

	redisdb_single = redis.connect {
		host = _conf.redis_host,
		port = _conf.redis_port,
		auth = _conf.redis_auth,
	}
	if not redisdb_single then
		skynet.error("clustermgr connect to redis fail")
		error("clustermgr connect to redis fail")
	end
	logger.info("clustermgr connect to redis success")

	--update newest nodelist
	local all_nodes = update_cluster()

	--start self node
	if nodeEndpoint then
		logger.info("clustermgr start self node, nodegroup = %s, nodeid = %s, nodename = %s, nodeip = %s, nodeport = %s, nodeFullName = %s",
			nodegroup, nodeid, nodeName, nodeIp, nodePort, nodeFullName)
		skynet.setenv("nodeFullName", nodeFullName)
		unregOldSelf()
		local clusterd = skynet.uniqueservice("lkclusterd")
		logger.info("clusterd opening port: %s", nodePort)
		local ok, err = pcall(skynet.call, clusterd, "lua", "listen", nodeLocalIp, nodePort)
		if not ok then
			local errMsg = string.format("listen_error:port=%s,name=%s", nodePort)
			logger.err(errMsg)
			skynet.error(errMsg)
			error(err)
		end
		if nodeid then
			local ok, other_node_name = check_nodeid(nodegroup, nodeid, nodeFullName)
			if not ok then
				error(string.format("nodeid is occupy by others, nodeid = %s, other_node_name = %s", nodeid, other_node_name))
			end
		end
		running = true
		reg_self()
		skynet.fork(repeat_reg_self)
		if nodeid then
			skynet.fork(repeat_update_nodeid)
		end
	end

	--notice other nodes
	skynet.fork(noticeExistedClusters, all_nodes)

	--do update while still running
	skynet.fork(repeat_update_cluster)

	skynet.info_func(function ()
		local strTbl = {}
		local cacheConf = cluster_cfg_mgr:get_all()
		for k,v in pairs(cacheConf) do
			table.insert(strTbl, string.format("nodename=%s, address=%s", k, v))
		end
		table.sort(strTbl)
		return table.concat(strTbl, "\n")
	end)
	logger.info("clustermgr started")
end)
