package com.myschool.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Archievement {
        // map class extend Mapper
    public static class preMap
        extends Mapper<Text, Text, Text, Text> {
        // override map function
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // this ignore class can judge a element(String) wether or not be filter
            // FileSplit fileSplit = (FileSplit)content.getInputSplit();
            // Get the filename of files. According to requirements, we need remove the suffix from the filename
            // String fileName = fileSplit.getPath().getName();

            //键值调换
            context.write(value, key);
            // String key_name = key.toString();
            // String value_str = value.toString();

            // value_str = value_str.substring(1, value_str.length() - 1);

            // // 将值中的各个出边所对应的节点取出来
            // String out_point_list[] = value_str.split("\\|");
            // for (String item : out_point_list) {
            //     String point_weight[] = item.split(",");
            //     String name = point_weight[0];
            //     String weight = point_weight[1];
            //     // 将每个出边对应的点作为key,这个出边点和权重作为value,它们以逗号分隔
            //     context.write(new Text(name), new Text(key_name + "," + weight));
            // }
        }
    }

    public static class preReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // split word and filename
            // String[] source = key.toString().split(",");
            // String word = source[0];
            // String filename = source[1];
            String result_value = new String();
            for (Text item : values) {
                if (result_value.length() == 0) {
                    result_value += item.toString();
                } else {
                    result_value += ("," + item.toString());
                }
            }
            context.write(key, new Text(result_value));
        }
    }
    public static void main(String[]args)throws Exception{
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "preprocessing");

        job1.setJarByClass(LabelPropagation.class);

        job1.setMapperClass(preMap.class);
        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(preReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

	    job1.waitForCompletion(true);
    }
}