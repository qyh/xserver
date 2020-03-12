-- This file will execute before every lua service start
-- See config
package.path = '../server/?.lua;../common/?.lua;../common/slaxml/?.lua;../res/?.lua;'
	.."../service/?.lua;bin/?.lua;../service/it_audit/?.lua;"
	..package.path..';'
package.cpath = package.cpath..";./cservice/?.so;../luaclib/?.so;"

