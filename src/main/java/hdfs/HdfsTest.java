package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 *  use java to read and write hdfs file
 */
public class HdfsTest{
    static FileSystem fs = null;
    static{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.17.0.2:9000/");
        try {
            fs = FileSystem.get(conf);
        }catch (IOException e){

        }finally {

        }
    }

    public static void main(String[] args) throws IOException{
        // 读取 hdfs 数据 并 输出到控制台
        //System.out.println("print console");
        //readFileToConsole("/hdfs-site.xml");

        // 读取 hdfs 数据, 并 输出到本地文件
        // System.out.println("print local");
        // readFileToLocal("/hdfs-site.xml", "/home/z3ero/bigdata/aa.txt");

        // 上传本地文件到 hdfs
        copyFromLocal("/home/z3ero/bigdata/aa.txt", "test/aa.txt");
    }

    public static void readFileToConsole(String path) throws IOException{
        // operate
        FSDataInputStream fis = fs.open(new Path(path));

        // hadoop 提供的对流的处理 默认处理 4k, true 表示完毕后， 自动关闭文件流
        IOUtils.copyBytes(fis, System.out, 4096, true);
    }

    /**
     * 从 hdfs 中读取文件，并保存到本地
     */
    public static void readFileToLocal(String readpath, String outpath) throws IOException{
        FSDataInputStream fis = null;
        OutputStream out = null;
        try {
            // get conf
            Configuration conf = new Configuration();
            // 另一种方式 打开 hdfs 文件操作对象
            FileSystem fs = FileSystem.get(new URI("hdfs://172.17.0.2:9000/"), conf, "root");

            fis = fs.open(new Path(readpath));
            out = new FileOutputStream(new File("/home/z3ero/bigdata/aa.txt"));

            IOUtils.copyBytes(fis, out, 4096, true);
        } catch(Exception e){

        }finally {
            fis.close();
            out.close();
        }
    }

    // 将本地文件 上传到 集群系统
    public static void copyFromLocal(String src_path, String dst_path) throws IOException{
        fs.copyFromLocalFile(new Path(src_path), new Path(dst_path));
        System.out.println("upload finished");
    }
}
