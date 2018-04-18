package com.bim.rpc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;


public class DeployService {

    public static void main(String[] args){

        try{

            RPC.Builder builder = new RPC.Builder(new Configuration());
            builder.setBindAddress("zyh").setPort(8094).setProtocol(LoginService.class).setInstance(new LoginServiceImpl());
            RPC.Server server = builder.build();
            server.start();

        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
