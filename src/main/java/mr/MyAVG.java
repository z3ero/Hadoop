package mr;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 使用 mapreduce求平均值
 * 求三个流水线 平均 每个时间段的 生产数据量
 *      S   M   E
 *  L1 200 600 700
 *  L2 400 900 800
 *  L3 300 600 900
 *
 * 结果：
 * L1 500
 * L2 700
 * L3 600
 */
public class MyAVG {

    // 框架仍然是 Mapper Reduce
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, FloatWritable>{

        private final static FloatWritable v = new FloatWritable(1);
        private Text k = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // 从输入数据中获取每一行的值
            String line = value.toString();
            // 对每一行数据进行切分
            String [] words = line.split("\t");
            String linename = words[0];
            int x = Integer.parseInt(words[1]);
            int y = Integer.parseInt(words[2]);
            int z = Integer.parseInt(words[3]);
            k.set(linename);
            v.set((float)(x+y+z)/(words.length-1));
            context.write(k, v);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context
        ) throws IOException, InterruptedException {
            context.write(key, new FloatWritable(values.iterator().next().get()));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "my avg");
        job.setJarByClass(MyAVG.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
