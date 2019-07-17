package mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  自定义 partitioner
 */
public class MyPartitioner extends Partitioner<Text, Text> {

    // 重写方法
    public int getPartition(Text key, Text value, int number_partion) {
        // 返回分区编号
        String fisrtChar = key.toString().substring(0,1);
        if (fisrtChar.matches("^[A-Z]")){
            return 0%number_partion;
        }else if(fisrtChar.matches("^[a-z]")){
            return 1%number_partion;
        }else if(fisrtChar.matches("^[0-9]")){
            return 2%number_partion;
        }else{
            return 3%number_partion;
        }
    }
    // 与 reduce 阶段输入是一样的


}
