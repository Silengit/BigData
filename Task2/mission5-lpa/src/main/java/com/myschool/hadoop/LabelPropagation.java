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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class LabelPropagation {


    private static HashMap<String, String> cluster_map = new HashMap<String, String>();
    private static Comparator<HashMap.Entry<String,Double>> comp = null;
    private static Set<String> keySet = null;

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
            Set<String> keySet = cluster_map.keySet();

            // 新建一个 comparator
            comp = new Comparator< HashMap.Entry<String,Double>>() {
                public int compare(HashMap.Entry<String,Double> o1, HashMap.Entry<String,Double> o2) {
                    return -o1.getValue().compareTo(o2.getValue());
                }
            };
        }
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // this ignore class can judge a element(String) wether or not be filter

            // FileSplit fileSplit = (FileSplit)content.getInputSplit();
            // Get the filename of files. According to requirements, we need remove the suffix from the filename
            // String fileName = fileSplit.getPath().getName();
            
            String key_name = key.toString();
            String value_name = value.toString();
            String value_list[] = value_name.substring(1, value_name.length() - 1).split("\\|");

            // HashMap<Double,String> tempmap = new HashMap<Double,String>();
            // for (String str : value_list) {
            //     String tuple[] = str.split(",");
            //     tempmap.put(Double.parseDouble(tuple[1]), tuple[0]);
            // }
            // List<HashMap.Entry<Double, String>> list = new ArrayList<HashMap.Entry<Double, String>>(tempmap.entrySet());
            // // 做降序排列，因此可以取出入度边权重最大的边，和对应的点
            // Collections.sort(list, comp);
            // 只有被测试姓名在姓名表中的情况下，才做记录
            if (cluster_map.keySet().contains(key_name)) {
                HashMap<String,Double> tempmap = new HashMap<String,Double>();

                for (String item : value_list) {
                    String tuple[] = item.split(",",2);
                    String label = cluster_map.get(tuple[0]);
                    if (label == null) {
                        continue;
                    }

                    if (tempmap.keySet().contains(label)) {
                        // tempmap[tuple[0]] += Double.parseDouble(tuple[1]);
                        tempmap.put(label, tempmap.get(label) + Double.parseDouble(tuple[1]));
                    } else {
                        tempmap.put(label, Double.parseDouble(tuple[1]));
                    }
                }
                if (tempmap.size() > 0) {
                    List<HashMap.Entry<String,Double>> list = new ArrayList<HashMap.Entry<String,Double>>(tempmap.entrySet());
                    Collections.sort(list, comp);
                    context.write(key, new Text(list.get(0).getKey()));
                }
                // neighber 将记录入边权重最大的边对应的点,下面的foreach是为了去除不是人名的点
                // String neighbor = null;
                // for (HashMap.Entry<Double, String> item : list) {
                //     if (cluster_map.keySet().contains(item.getValue())) {
                //         neighbor = item.getValue();
                //         break;
                //     }
                // }
                // if (neighbor == null) {
                //     // 如果某个点的入边邻居都被排除了，那么这个点的标签不变
                //     context.write(key, new Text(cluster_map.get(key.toString())));
                //     // context.write(new Text("状态: "), new Text("不变"));
                // } else {
                //     context.write(key, new Text(cluster_map.get(neighbor)));
                //     // context.write(new Text("状态: "), new Text("更新"));
                // }
            }
            // context.write(new Text("状态: "), new Text("这个键不是一个名字"));
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
	    Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "LPA");

        job2.setJarByClass(LabelPropagation.class);

        job2.setMapperClass(Map.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(Reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        // 设置聚类信息的文件路径
        job2.getConfiguration().set("cluster", args[2]);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

	    job2.waitForCompletion(true);

    }
}