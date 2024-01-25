package mapreduce.flowsum;

import bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 获取1行
        String line = value.toString();

        //2. 切割字段
        String[] fields = line.split("\t");

        //3. 获取数据
        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        //4. 改变kv
        k.set(phoneNum);
        v.set(upFlow,downFlow);

        //4. 写入
        context.write(k,v);
    }
}
