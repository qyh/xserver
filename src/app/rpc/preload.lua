-- This file will execute before every lua service start
-- See config
package.path = './service/?.lua;../src/app/test/?.lua;../src/common/?.lua;../src/common/slaxml/?.lua;../res/?.lua;'
	.."../src/service/?.lua;../src/service/it_audit/?.lua;"
	..package.path..';'
package.cpath = package.cpath..";./cservice/?.so;../luaclib/?.so;"

