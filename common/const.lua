local const = {}

const.loglevel = {
    debug = 1,
    info  = 2,
    warn  = 3,
    err   = 4,
}

const.error_code = {
    ok = 0,
    http_req_fail = 1,
    http_data_error = 2,
}

const.pubsubChannel = {
	pub_test = "pub_test",
	WinChallengeConfigUpdate = "WinChallengeConfigUpdate",
	WinChallengeMgLock = "WinChallengeMgLock",
	client_service = 'client_service',
    ch_release_lock = 'ch_release_lock',
    ch_new_challenge_match = "ch_new_challenge_match",
    ch_new_challenge_sign_in = "ch_new_challenge_sign_in",
}
return const
