package com.myschool.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Archievement {
        // map class extend Mapper
    public static class Map
        extends Mapper<Text, Text, Text, Text> {
        // override map function
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //键值调换
            context.write(value, key);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "archievement");
        job.setJarByClass(Archievement.class);
        job.setMapperClass(Map.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);
    }
}