package com.template.hdfs;

import com.template.RunParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.Map;

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
//                    printRootFile(fs);
                    testMkdirs(fs);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        tempRunHDFSOperate(toRun);

    }

    private static final String TEST_ROOT_DIR = "/test";

    private static void testMkdirs(FileSystem fs) throws IOException {

        Path test_dir = new Path(TEST_ROOT_DIR, "test_dir");
        Path test_file = new Path(TEST_ROOT_DIR, "file1");

        if (fs.mkdirs(test_dir)) {
            /*解决文件无法上传，真实的环境，应该不会出现这样，因为window的环境，没有放入真实的用户名*/
            //http://blog.csdn.net/xjdalan/article/details/49802937
            FSDataOutputStream stm = fs.create(test_file);
            stm.writeBytes("42\n");
            stm.close();
        }
    }

    private static void printRootFile(FileSystem fs) throws Exception {
//        RemoteIterator<LocatedFileStatus> itor = fs.listFiles(new Path("/"), false);
        RemoteIterator<LocatedFileStatus> itor = fs.listFiles(new Path("/"), true);
        while (itor.hasNext()) {
            LocatedFileStatus fileStatus = itor.next();
            String name = fileStatus.getPath().getName();
            System.out.println(fileStatus.getPath().getParent());
            System.out.println(name);
        }
    }

    private static void tempRunHDFSOperate(RunParam toRun) throws IOException {
        /*此处的配置是因为你的resources
        有core-site.xml，hdfs-site.xml进行配置，
        不然默认是读取local*/
        Configuration conf = new Configuration();

        System.out.println(conf.get("fs.defaultFS"));
        FileSystem fs = FileSystem.get(conf);

        toRun.run(fs);

        fs.close();
    }


}
