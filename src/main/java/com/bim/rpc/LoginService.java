package com.bim.rpc;


/**
 * hadoop RPC发布应用服务
 */
public interface LoginService {

    /**
     * 定义版本号
     */
    public static final long versionID = 1L;

    String login(String username,String password);

}
