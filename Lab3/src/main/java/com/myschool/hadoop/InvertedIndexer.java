package com.myschool.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class InvertedIndexer {
    
    public static class Map extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            Text fileName_lineOffset = new Text(fileName+"#"+key.toString());
            StringTokenizer itr = new StringTokenizer(value.toString());
            for(;itr.hasMoreTokens();)
            {
                word.set(itr.nextToken());
                context.write(word, fileName_lineOffset);
            }
        }
    }

    public static class Reduce extends Reducer <Text, Text, Text, Text>  {
        @Override
        public void reduce(Text key, Iterable<Text> values, Reducer <Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            StringBuilder all = new StringBuilder();
            if(it.hasNext())
                all.append(it.next().toString());
            for(;it.hasNext();)
            {
                all.append(";");
                all.append(it.next().toString());
            }
            context.write(key, new Text(all.toString()));
        }
    }

    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "invert index");
        job.setJarByClass(InvertedIndexer.class);
        
        //job.setInputFormatClass(TextInputFormat.class);
        
        //map 
        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reduce
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
	