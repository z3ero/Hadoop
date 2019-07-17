package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * rpc 中的客户端
 */
public class RPCClient {
    public static void main(String[] args) {
        try{
            System.out.println("nihao");
            // 获取协议
            Hello hello = RPC.getProxy(Hello.class, 1, new InetSocketAddress("127.0.0.1", 6666), new Configuration());
            // 调用协议中的方法
            String res = hello.say("i am datanode01, i am live");
            System.out.println(res);
            Thread.sleep(2000);
        }catch(IOException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
