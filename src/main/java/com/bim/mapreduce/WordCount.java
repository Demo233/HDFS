package com.bim.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

public class WordCount {
    static class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
        /**
         *
         * @param key 每次map读取一行的起始偏移量
         * @param value 每次map读取的一行数据
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word:words) {
                context.write(new Text(word),new LongWritable(1));
            }
        }
    }

    static class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        /**
         * <java,1><java,1><java,1><java,1><java,1><java,1><java,1>
         * <c,1><c,1><c,1><c,1>
         * <asd,1><asd,1><asd,1><asd,1><asd,1><asd,1>
         * @param key java
         * @param values 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value:values) {
                count += value.get();
            }
            context.write(key,new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        // 设置文件系统是本地
        //conf.set("fs.defaultFS", "local");

        // 设置mr运行环境是本地
        //conf.set("mapreduce.framework.name","local");

        // 默认就是local所以我们可以省略

        Job job = Job.getInstance(conf);
        // 指定本程序jar包所在的路径
        job.setJarByClass(WordCount.class);
        // 设置mapper
        job.setMapperClass(WordCountMapper.class);
        // 设置reduce
        job.setReducerClass(WordCountReducer.class);

        // 指定maptask的输出结果kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 指定reducetask的输出结果kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 获取用户输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/home/zyh/wc/input/"));
        FileOutputFormat.setOutputPath(job,new Path("/home/zyh/wc/output6/"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }

}