package com.bim.mapreduce.commonFriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:F,A,D,I
 * D:A,E,F,L
 * E:B,C,D,M,L
 * F:A,B,C,D,E,O,M
 * G:A,C,D,E,F
 * H:A,C,D,E,O
 * I:A,O
 * J:B,O
 * K:A,C,D
 * L:D,E,F
 * M:E,F,G
 * O:A,H,I,J

 * 求出哪些人两两之间有共同好友，及他俩的共同好友都是谁
 * 比如:
 * a-b :  c ,e
 */
public class CommonFriendOne {
    /*
     思路:1
     maptask
        B -> A ,C - > A,D->A,B -> E,C->K...
     reducetask
        b:A,E
        c:A,K
        D:A
     maptask
        (A,E)->B , (A,K) - > C
     reducetask
        (A,E)->B,D


     思路:2 没走通后续有时间看一下 Caused by: java.lang.ArrayIndexOutOfBoundsException: 1
     maptask
        B -> A ,C - > A,D->A,B -> E,C->K...
     reducetask
        (A,E)->V , (A,K) - > C
     maptask
        原样输出
     reducetask
        (A,E)->B,D
     */
    static class CommonFriendMapperOne extends Mapper<LongWritable,Text,Text,Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // A:B,C,D,F,E,O
            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String friend = person_friends[1];

            for (String str:friend.split(",")) {
                k.set(str);
                v.set(person);
                context.write(k,v);
            }
        }
    }

    static class CommonFriendReduceOne extends Reducer<Text,Text,Text,Text>{

        Text k = new Text();
        Text v = new Text();

        /**
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();
            Iterator<Text> iterator = values.iterator();
            while(iterator.hasNext()){
                String value = iterator.next().toString();
                sb.append(value+",");
            }
            k.set(key);
            v.set(sb.toString());
            context.write(k,v);

        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CommonFriendOne.class);

        // 设置mapper和reducer
        job.setMapperClass(CommonFriendMapperOne.class);
        job.setReducerClass(CommonFriendReduceOne.class);

        // 设置输入输出目录
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置文件的下载目录和计算输出目录
        FileInputFormat.setInputPaths(job,new Path("/home/zyh/Documents/bigdata/homework/commonfriend.log"));
        FileOutputFormat.setOutputPath(job,new Path("/home/zyh/Documents/bigdata/homework/commonfriendoutput/"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }

}



















