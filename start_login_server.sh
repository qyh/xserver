#!/bin/bash
param=$1
echo "param $param"
if [[ "-d" == "$param" ]]; then
	cd skynet && ./skynet ../config/config.daemon
else
	cd skynet && ./skynet ../config/config.login
fi
