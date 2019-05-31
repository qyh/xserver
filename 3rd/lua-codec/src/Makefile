all:
	#gcc -Wall -O2 -shared -o codec.so codec.c -I /usr/include -lcrypto -fPIC -std=c99
	gcc -Wall -O2 -shared -o codec.so codec.c ./openssl/libssl.a ./openssl/libcrypto.a -I ./openssl/include/openssl -I ../../skynet/3rd/lua -fPIC -std=c99
	cp codec.so ../../../luaclib/ -rf
clean:
	rm -f codec.so
