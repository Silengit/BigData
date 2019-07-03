package com.myschool.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class LabelPropagation {
    // map class extend Mapper
    public static class preMap
        extends Mapper<Object, Text, Text, Text> {
        // override map function
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // this ignore class can judge a element(String) wether or not be filter
            // FileSplit fileSplit = (FileSplit)content.getInputSplit();
            // Get the filename of files. According to requirements, we need remove the suffix from the filename
            // String fileName = fileSplit.getPath().getName();
            // value 是形如 "大圣   [孙悟空,0.3333 | 孙悟饭,0.33333]"这样的出边邻接表
            String str = value.toString();
            String tuple[] = str.split(" ", 2);
            // 获取 键 - 值,这里的值就是 "[孙悟空,0.3333 | 孙悟饭,0.33333]"
            String key_name = tuple[0];
            String value_str = tuple[1];
            // 将值中的各个出边所对应的节点取出来
            String out_point_list[] = value_str.split("\\|");
            for (String item : out_point_list) {
                String point_weight[] = item.split(",");
                String name = point_weight[0];
                String weight = point_weight[1];
                // 将每个出边对应的点作为key,这个出边点和权重作为value,它们以逗号分隔
                context.write(new Text(name), new Text(key_name + "," + weight));
            }
        }
    }

    public static class preReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // split word and filename
            // String[] source = key.toString().split(",");
            // String word = source[0];
            // String filename = source[1];
            String result_value = new String();
            for (Text item : values) {
                if (result_value.length() == 0) {
                    result_value += item.toString();
                }
                else {
                    result_value += ("|" + item.toString());
                }
            }
            context.write(key, new Text(result_value));
        }
    }
    // map class extend Mapper
    public static class Map
        extends Mapper<Object, Text, Text, IntWritable> {

        private static Text Key_word = new Text();
        private static IntWritable Value_int = new IntWritable();

        // override map function
        @Override
        protected void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            // this ignore class can judge a element(String) wether or not be filter

            FileSplit fileSplit = (FileSplit)content.getInputSplit();
            // Get the filename of files. According to requirements, we need remove the suffix from the filename
            String fileName = fileSplit.getPath().getName();

        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

        private static Text Key_word = new Text();
        private static Text textValue = new Text();

        private static String curWord = null;
        private static int appear_num = 0;
        private static int file_num = 0;
        StringBuffer ValueText = new StringBuffer();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // split word and filename
            String[] source = key.toString().split(",");
            String word = source[0];
            String filename = source[1];

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "preprocessing");

        job1.setJarByClass(LabelPropagation.class);

        job1.setMapperClass(preMap.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(preReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

	    // job2
	    // Configuration conf2 = new Configuration();

        // Job job2 = Job.getInstance(conf2, "Inverted File");

        // job2.setJarByClass(LabelPropagation.class);

        // job2.setMapperClass(Map.class);
        // job2.setMapOutputKeyClass(Text.class);
        // job2.setMapOutputValueClass(IntWritable.class);

        // job2.setReducerClass(Reduce.class);
        // job2.setOutputKeyClass(Text.class);
        // job2.setOutputValueClass(Text.class);

        // FileInputFormat.addInputPath(job2, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        // System.exit(job1.waitForCompletion(true) ? 0 : 1);
	    job1.waitForCompletion(true);
	    // job2.waitForCompletion(true);

    }
}