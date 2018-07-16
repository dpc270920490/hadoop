package cn.qzt.test;

import org.apache.hadoop.io.LongWritable;

import java.util.Iterator;

/**
 * Title:  hadoopdemo
 * Description:  SSD
 *
 * @author dc
 * @date2018/7/1515:13
 */
public class Test {
    @org.junit.Test
    public void test(){
        LongWritable one = new LongWritable(8);
        System.out.println(one.get());
    }
}
