package rpc;

/*
 * rpc 中的协议
 */

public interface Hello {
    public static final long versionID = 1;
    public String say(String words);
}
