package com.template.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Created by huanghh on 2017/1/25.
 */
public class WcReduce extends Reducer<Text,LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        final AtomicLong count = new AtomicLong();
        values.forEach(new Consumer<LongWritable>() {
            @Override
            public void accept(LongWritable longWritable) {
                count.addAndGet(longWritable.get());
            }
        });

        context.write(key,new LongWritable(count.longValue()));
    }
}
