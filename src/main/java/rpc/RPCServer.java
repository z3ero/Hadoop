package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

/**
 * RPC 中的server
 */
public class RPCServer implements Hello{

    public String say(String words){
        System.out.println(words);
        // 收到 words 后的逻辑代码
        return "received datanode01 heartbeats!";
    }

    public static void main(String[] args){
        try{
            // 获取一个服务
            Server server = new RPC.Builder(new Configuration())
                    .setInstance(new RPCServer())
                    .setProtocol(Hello.class)
                    .setBindAddress("127.0.0.1")
                    .setPort(6666)
                    .build();
            System.out.println("server is started...");
        }catch (Exception e){
            // TODO
        }
    }


}
