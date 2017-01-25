package com.template.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by huanghh on 2017/1/25.
 */
public class WcMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word :
                words) {
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
