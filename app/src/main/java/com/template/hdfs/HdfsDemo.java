package com.template.hdfs;

import com.template.RunParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

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
                    printRootFile(fs);

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };

        tempRunHDFSOperate(toRun);

    }

    private static void printRootFile(FileSystem fs) throws Exception {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), false);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            String name = locatedFileStatus.getPath().getName();
            System.out.println(name);
        }
    }

    private static void tempRunHDFSOperate(RunParam toRun) throws IOException {
        Configuration conf = new Configuration();//此处的配置是因为你的resources有core-site.xml，hdfs-site.xml进行配置，不然默认是读取local
        System.out.println(conf.get("fs.defaultFS"));
        FileSystem fs = FileSystem.get(conf);

        toRun.run(fs);

        fs.close();
    }



}
