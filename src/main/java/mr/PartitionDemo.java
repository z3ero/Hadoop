package mr;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 自定义分区
 * 输入数据：
 * Hello HI HI qianfeng
 * hi hi qiangfeng qianfeng
 * 163.com
 * 189.com
 * @163.com
 * @qq.com
 * *123
 *
 * 输出4个文件
 * 1. 大写字母打头 eg：Hello 1
 * 2. 小写字母打头
 * 3. 数字打头
 * 4. 特殊字符打头
 *
 * 注意：
 * 1. 分区需要继承 Partitioner<key, value>, 其中的 key-value 需要和map阶段的输出相同
 * 2. 实现 getPartition(key, value, partition_num)方法，该方法只返回 int 类型
 * 3. 分区数 与 reduce 个数相同
 * 4. 默认使用 HashPartitioner
 */

public class PartitionDemo {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String words [] = line.split(" ");
            for (String s: words){
                context.write(new Text(s), new Text(1+""));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int counter = 0;
            for (Text t : values) {
                counter += Integer.parseInt(t.toString());
            }
            context.write(key, new Text(counter+""));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "my partition");
        job.setJarByClass(PartitionDemo.class);
        job.setMapperClass(PartitionDemo.TokenizerMapper.class);
        job.setCombinerClass(PartitionDemo.IntSumReducer.class);
        job.setReducerClass(PartitionDemo.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 添加分区信息
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
