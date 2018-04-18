package com.bim.rpc;

public class LoginServiceImpl implements LoginService {


    public String login(String username, String password) {

        System.out.println("用户名"+username);
        System.out.println("登录验证.......");

        return "登录成功";
    }
}
