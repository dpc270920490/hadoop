package com.qzt.mapreduce;

import org.apache.commons.collections.IterableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Title:  hadoopdemo
 * Description:  SSD
 *
 * @author dc
 * @date2018/7/1216:57
 */
public class WordCountApp {
    /**
     * 实现mapper
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
        LongWritable one = new LongWritable(1);

        @Override
        public void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws IOException, InterruptedException {
            //读取的每一行数据
            String line = value.toString();
            //按照分隔符，分隔数据
            String[] words = line.split("\t");

            for(String word :words){
                //将分隔的数据进行输出
                context.write(new Text(word),one);
            }

        }


    }

    /**
     * 实现reduce
     */
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {

            long sum = 0;

            for (LongWritable value : values){

                sum+=value.get();

            }
//输出结果
            context.write(key,new LongWritable(sum));

        }



    }

    /**
     * 整合mapper与reduce
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        //清除已存在的输出文件目录
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(outputPath)){

            fs.delete(outputPath,true);
            System.out.println("output file exists, but it has delete");

        }

        
        //创建job
        Job job = Job.getInstance(configuration,"wordcount");

        //设置job处理类
        job.setJarByClass(WordCountApp.class);
        //设置作业处理输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //设置mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输出
        FileInputFormat.setInputPaths(job,args[1]);

        boolean resule =   job.waitForCompletion(true);

        System.exit(resule ? 0 : 1);
    }

}
