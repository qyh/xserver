-- This file will execute before every lua service start
-- See config
package.path = './service/?.lua;'
..'../src/app/connector/?.lua;'
..'../src/common/?.lua;'
..'../src/service/?.lua;'
..'../src/service/cluster/?.lua;'
..package.path..';'
package.cpath = package.cpath..";./cservice/?.so;../luaclib/?.so;"

