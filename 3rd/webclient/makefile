
LUA_INCLUDE_DIR = ../../skynet-1.0.0/3rd/lua
CURL_INCLUDE_DIR = ./curl/include/curl
CURL_LIB_DIR = ./curl/lib/.libs

webclient_core.so:
	#gcc -Wall -O2 -std=c99 webclient.c -o webclient_core.so -shared -fPIC -I$(LUA_INCLUDE_DIR) -I$(CURL_INCLUDE_DIR) -L$(CURL_LIB_DIR)  -lcurl
	gcc -Wall -O2 -std=c99 webclient.c ./libcurl.a -o webclient_core.so -shared -fPIC -I$(LUA_INCLUDE_DIR) -I$(CURL_INCLUDE_DIR) -L$(CURL_LIB_DIR)  -lcurl
	cp webclient_core.so ../../luaclib/ -rf
	cp webclient.lua ../../service/webclient.lua

clean:
	rm -rf *.so *.o
