# -*- coding:utf8 -*
import os,sys,time
import urllib, urllib2, sys
import ssl
import json
import redis
import MySQLdb
from multiprocessing import Process
reload(sys)
sys.setdefaultencoding('utf8')   
def daemon_init(stdin='/dev/null',stdout='/dev/null',stderr='/dev/null'):
	sys.stdin = open(stdin,'r')
	sys.stdout = open(stdout,'a+')
	sys.stderr = open(stderr,'a+')
	try:
		pid = os.fork()
		if pid > 0:		#parrent
			os._exit(0)
	except OSError,e:
		sys.stderr.write("first fork failed!!"+e.strerror)
		os._exit(1)
		 
	# 子进程， 由于父进程已经退出，所以子进程变为孤儿进程，由init收养
	'''setsid使子进程成为新的会话首进程，和进程组的组长，与原来的进程组、控制终端和登录会话脱离。'''
	os.setsid()
	'''防止在类似于临时挂载的文件系统下运行，例如/mnt文件夹下，这样守护进程一旦运行，临时挂载的文件系统就无法卸载了，这里我们推荐把当前工作目录切换到根目录下'''
	#os.chdir("/")
	'''设置用户创建文件的默认权限，设置的是权限“补码”，这里将文件权限掩码设为0，使得用户创建的文件具有最大的权限。否则，默认权限是从父进程继承得来的'''
	os.umask(0)
 
	try:
		pid = os.fork()	 #第二次进行fork,为了防止会话首进程意外获得控制终端
		if pid > 0:
			os._exit(0)	 #父进程退出
	except OSError,e:
		sys.stderr.write("second fork failed!!"+e.strerror)
		os._exit(1)
 
	# 孙进程
	#   for i in range(3,64):  # 关闭所有可能打开的不需要的文件，UNP中这样处理，但是发现在python中实现不需要。
	#	   os.close(i)
	sys.stdout.write("Daemon has been created! with pid: %d\n" % os.getpid())
	sys.stdout.flush()  #由于这里我们使用的是标准IO，回顾APUE第五章，这里应该是行缓冲或全缓冲，因此要调用flush，从内存中刷入日志文件。

'''
import time
#获得当前时间时间戳
now = int(time.time())
#转换为其他日期格式,如:"%Y-%m-%d %H:%M:%S"
timeStruct = time.localtime(now)
strTime = time.strftime("%Y-%m-%d %H:%M:%S", timeStruct)
print(strTime)
'''
class Logger:
	def get_cur_time(self):
		now = int(time.time())
		time_st = time.localtime(now)
		str = time.strftime("%Y-%m-%d %H:%M:%S", time_st)
		return str
	def info(self, *args):
		sys.stdout.write('[{}][info] {}\n'.format(self.get_cur_time(), *args))
		sys.stdout.flush() 
	def error(self, *args):
		sys.stderr.write('[{}][error] {}\n'.format(self.get_cur_time(), *args))
		sys.stderr.flush() 
	def warn(self, *args):
		sys.stdout.write('[{}][warn] {}\n'.format(self.get_cur_time(), *args))
		sys.stdout.flush() 
	def debug(self, *args):
		sys.stdout.write('[{}][debug] {}\n'.format(self.get_cur_time(), *args))
		sys.stdout.flush() 


name = "mipcheck"
pid = "./{}.pid".format(name)
logger = Logger()
log = "./log/{}.log".format(name)
err = "./log/err.log"

def main():
	print '========main function start!============' #在调用daemon_init函数前是可以使用print到标准输出的，调用之后就要用把提示信息通过stdout发送到日志系统中了
	daemon_init('/dev/null', log, err)	# 调用之后，你的程序已经成为了一个守护进程，可以执行自己的程序入口
	_pid = open(pid, "w")
	_pid.write("{}".format(os.getpid()))
	_pid.flush()
	_pid.close()

results = {}
rhost = '127.0.0.1'
r = redis.Redis(host=rhost, port=6379)
host = 'https://ipcheck.market.alicloudapi.com'
path = '/convertip'
method = 'POST'
appcode = '2ba2734a68034076b7ffc50641217da5'
querys = ''
url = host + path

def run(id, args):
	logger.debug("pro:{}".format(id))
	logger.debug("args:", args)
	for userID, amount in res:
		rdsKey = "user_ip:" + str(userID)
		regIp = r.get(rdsKey)
		logger.debug("userID:{}, amount{}, id:{}, ip:{}".format(userID, amount, id, regIp))
		if regIp:
			rdsKey_pay = "pay_user_ip:" + str(userID)
			rv = r.get(rdsKey_pay)
			if not rv:
				bodys = {}
				bodys['src'] = regIp 
				post_data = urllib.urlencode(bodys)
				request = urllib2.Request(url, post_data)
				request.add_header('Authorization', 'APPCODE ' + appcode)
				request.add_header('Content-Type', 'application/x-www-form-urlencoded; charset=UTF-8')
				ctx = ssl.create_default_context()
				ctx.check_hostname = False
				ctx.verify_mode = ssl.CERT_NONE
				response = urllib2.urlopen(request, context=ctx)
				content = response.read()
				if (content):
					obj = json.loads(content)
					rkey = rdsKey_pay
					ok = r.set(rkey, obj['msg'.encode('utf8')])
					logger.debug("set user:{} {} success:{}".format(userID, obj['msg'.encode('utf8')], ok))
	logger.debug("pro:{} done".format(id))
if __name__ == '__main__':
	main()
	idx = 0
	rdsKey = "RechargeRank"
	while True:
		print('query from idx:', idx)
		res = r.zscan(rdsKey, idx, "*", 50000)
		idx = int(res[0])
		if idx == 0:
			break
		results[idx] = res[1]
	pros = {}
	for k, res in results.items():
		p = Process(target=run, args=(k, res))
		p.start()
		pros[k] = p
	for k, p in pros.items():
		logger.debug("join:{}".format(k))
		p.join()
	logger.debug("all done !!!")
	


