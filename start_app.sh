#!/bin/bash
param=$1
if [[ -z "$param" ]]; then
	echo "please specified job[audit_recharge|audit_active_day|audit_game_win_lose]"
	exit 1
fi
cd skynet && ./skynet ../config/"config.$param"
