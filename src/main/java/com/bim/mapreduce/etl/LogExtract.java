package com.bim.mapreduce.etl;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class LogExtract {

    static class LogExtractMapper extends Mapper<LongWritable,Text,LongWritable,Text>{

        LogParser logParser = new LogParser();
        Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String[] parsed = logParser.parse(value.toString());

            // 过滤掉静态资源访问请求
            if (parsed[2].startsWith("GET /static/")
                    || parsed[2].startsWith("GET /assets/")) {
                return;
            }
            // 过滤掉开头的指定字符串
            if (parsed[2].startsWith("GET ")) {
                parsed[2] = parsed[2].substring("GET ".length());
            } else if (parsed[2].startsWith("POST ")) {
                parsed[2] = parsed[2].substring("POST ".length());
            } else if (parsed[2].startsWith("HEAD ")) {
                parsed[2] = parsed[2].substring("HEAD ".length());
            }

            // 过滤掉结尾的特定字符串
            String[] splits = parsed[2].split(" ");

            // 只写入前三个记录类型项
            outputValue.set(parsed[0] + "," + parsed[1] + "," + splits[0] + "," + splits[4] + "," + "1");
            context.write(key, outputValue);

        }
    }

    static class LogExtractReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v2 : values) {
                context.write(v2, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LogExtract.class);

        // 设置mapper和reducer
        job.setMapperClass(LogExtractMapper.class);
        job.setReducerClass(LogExtractReducer.class);

        // 设置输入输出目录
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置文件的下载目录和计算输出目录
        //FileInputFormat.setInputPaths(job,new Path("/home/zyh/workspace/0511test/test.log"));
        //FileOutputFormat.setOutputPath(job,new Path("/home/zyh/workspace/0511test/output"));
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }


    static class LogParser {
        public static final SimpleDateFormat FORMAT = new SimpleDateFormat(
                "d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        public static final SimpleDateFormat dateformat1 = new SimpleDateFormat(
                "yyyyMMddHHmmss");/**
         * 解析英文时间字符串
         *
         * @param string
         * @return
         * @throws ParseException
         */
        private Date parseDateFormat(String string) {
            Date parse = null;
            try {
                parse = FORMAT.parse(string);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return parse;
        }

        /**
         * 解析日志的行记录
         *
         * @param line
         * @return 分别是ip、时间、url、状态
         */
        public String[] parse(String line) {
            String ip = parseIP(line);
            String time = parseTime(line);
            String url = parseURL(line);
            String status = parseStatus(line);
            //String traffic = parseTraffic(line);

            return new String[] { ip, time, url, status };
        }

        /*private String parseTraffic(String line) {
            final String trim = line.substring(line.lastIndexOf("\"") + 1)
                    .trim();
            String traffic = trim.split(" ")[1];
            return traffic;
        }*/

        private String parseStatus(String line) {
            final String trim = line.substring(line.lastIndexOf("\"") + 1)
                    .trim();
            String status = trim.split(" ")[0];
            return status;
        }

        private String parseURL(String line) {
            final int first = line.indexOf("\"");
            final int last = line.lastIndexOf("\"");
            String url = line.substring(first + 1, last);
            return url;
        }

        private String parseTime(String line) {
            final int first = line.indexOf("[");
            final int last = line.indexOf("+0800]");
            String time = line.substring(first + 1, last).trim();
            Date date = parseDateFormat(time);
            return date.getDate() + "";
        }

        private String parseIP(String line) {
            String ip = line.split("- -")[0].trim();
            return ip;
        }
    }

}