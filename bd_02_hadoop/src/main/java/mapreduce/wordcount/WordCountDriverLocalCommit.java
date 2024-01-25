package mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriverLocalCommit {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.set("yarn.resourcemanager.hostname","hadoop103");


        Job job = Job.getInstance(conf);


        job.setJarByClass(WordCountDriverLocalCommit.class);
        job.setJar("D:\\DataScience\\Code\\big_data_component\\bd_02_hadoop\\target\\bd_02_hadoop-1.0-SNAPSHOT.jar");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));*/

        FileInputFormat.setInputPaths(job, new Path("/tmp/spark/input"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
