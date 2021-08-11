local error_code = {
    OK = 0,                 --成功,正常
    TIMEOUT = 1,            --超时
    NODE_NOT_FOUND = 2,     --结点不存在,不可达
    MAX = 65535,            --最大错误码,不能比这个大了
}


return error_code 
