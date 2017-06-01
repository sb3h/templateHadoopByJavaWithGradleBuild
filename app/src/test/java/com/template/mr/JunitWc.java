package com.template.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Created by huanghh on 2017/1/25.
 */
public class JunitWc {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length == 0){

            //window local test
            args[0] = "file:///d:/temp/hello.txt";
            args[1] = "file:///d:/temp/outHello-"+ UUID.randomUUID();
            if (!new File(args[0]).exists()){
                return;
            }
        }
        Configuration conf = new Configuration();//此处的配置是因为你的resources有core-site.xml，hdfs-site.xml进行配置，不然默认是读取local

        Job job = Job.getInstance(conf);
        /**
         * 指定
         * 运行的任务主类
         * 运行的Mapper
         * 运行的Reduce
         */
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReduce.class);
        job.setJarByClass(JunitWc.class);
        /**
         * 指定mapper
         * 输出的key
         * 输出的value
         */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        /**
         * 指定Reduce
         * 输出的key
         * 输出的value
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        /**
         * 指定输入文件路径
         */
        FileInputFormat.setInputPaths(job, args[0]);
        /**
         * 指定输出文件路径，
         * 注意一定要是一个新的文件夹
         */
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        /**
         * 启动
         */
        job.waitForCompletion(true);

    }
}
