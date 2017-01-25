package com.template;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by huanghh on 2017/1/24.
 */
public class HdfsDemo {


    public static void main(String[] args) throws IOException {
//        System.out.println("123");

        RunParam toRun = new RunParam<FileSystem>() {
            @Override
            public void run(FileSystem fs) {
                try {


                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };

        tempRunHDFSOperate(toRun);

    }

    private static void tempRunHDFSOperate(RunParam toRun) throws IOException {
        Configuration conf = new Configuration();//此处的配置是因为你的resources有core-site.xml，hdfs-site.xml进行配置，不然默认是读取local
        System.out.println(conf.get("fs.defaultFS"));
        FileSystem fs = FileSystem.get(conf);

        toRun.run(fs);

        fs.close();
    }



}
