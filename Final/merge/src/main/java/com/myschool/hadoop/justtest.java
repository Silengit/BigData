package com.myschool.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class justtest {
    private static HashMap<String, String> cluster_map = new HashMap<String, String>();
    private static Comparator<HashMap.Entry<String,Double>> comp = null;
    // map class extend Mapper
    public static class Map
        extends Mapper<Text, Text, Text, Text> {
        // override map function
        @Override
        protected void setup(Mapper<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            // 读取聚类信息
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            String clusterPath = context.getConfiguration().get("cluster");

            FSDataInputStream in = hdfs.open(new Path(clusterPath));
            Scanner scan = new Scanner(in);
            while (scan.hasNext()) {
                String str = scan.nextLine();
                String tuple[] = str.split("\\s+");
                cluster_map.put(tuple[0], tuple[1]);
            }
            scan.close();
            // 新建一个 comparator
            comp = new Comparator< HashMap.Entry<String,Double>>() {
                public int compare(HashMap.Entry<String,Double> o1, HashMap.Entry<String,Double> o2) {
                    return -o1.getValue().compareTo(o2.getValue());
                }
            };
        }
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {  
            String key_name = key.toString();
            String value_name = value.toString();
            // String value_list[] = value_name.split("\\s+")[1].split(";");
            //"大圣 孙悟空,0.333;孙悟饭,0.23"
            // 只有被测试姓名在姓名表中的情况下，才做记录
            if (value_name.length() == 0) {
                // 如果 value 的长度为0，则忽略
                return;
            }
            context.write(new Text(new String("1_" + value_name.length())), value);
        }
    }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        StringBuffer ValueText = new StringBuffer();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // reduce 什么也不做
            for (Text str : values) {
                context.write(key, str);
            }
        }
    }
    public static void main(String[] args) throws Exception {
	    // job2
	    Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPA");
        job.setJarByClass(LabelPropagation.class);

        job.setMapperClass(Map.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置聚类信息的文件路径
        job.getConfiguration().set("cluster", args[2]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);
    }
}