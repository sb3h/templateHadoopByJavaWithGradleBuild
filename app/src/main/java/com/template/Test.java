package com.template;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Created by huanghh on 2017/1/25.
 */
public class Test {
    public static void main(String[] args) {
//        String line = "1,2,3,5,6,7,8";
//
//        String[] nums = line.split(",");
//        List<String> numList = Arrays.asList(nums);
//        AtomicLong count = new AtomicLong();
//        numList.forEach(new Consumer<String>() {
//            @Override
//            public void accept(String s) {
//                count.addAndGet(Long.valueOf(s));
//            }
//        });
//        System.out.println(count.longValue());
        File f = new File(".");
        System.out.println(f.getAbsolutePath());
    }
}
