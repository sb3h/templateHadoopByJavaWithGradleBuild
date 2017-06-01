package com.template.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by sam3h on 2016/7/5 0005.
 */
public class JunitHDFS {
    FileSystem fs = null;

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();//此处的配置是因为你的resources有core-site.xml，hdfs-site.xml进行配置，不然默认是读取local

        fs = FileSystem.get(conf);
    }

    @Test
    public void testRun() throws IOException {

    }

    private static final String TEST_ROOT_DIR = "/test";

    @Test
    public void testMkdirs() throws IOException {

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

    @Test
    public void testPrintRootFile() throws Exception {
//        RemoteIterator<LocatedFileStatus> itor = fs.listFiles(new Path("/"), false);
        RemoteIterator<LocatedFileStatus> itor = fs.listFiles(new Path("/"), true);
        while (itor.hasNext()) {
            LocatedFileStatus fileStatus = itor.next();
            String name = fileStatus.getPath().getName();
            System.out.println(fileStatus.getPath().getParent());
            System.out.println(name);
        }
    }

    @After
    public void end() throws IOException {
        fs.close();
    }
}
