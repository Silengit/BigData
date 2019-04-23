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
    private static Set<String> initIgnoredWord() {
        /**
         * 这是用于初始化 "忽略字符"的函数,需要被忽略的词语通过 add 的方法加入 ignored_word 这个set
         * 在map函数中将 先检查 一个词是否在 ignore_word中，如果在，则忽略
         */
        
        Set<String> ignored = new HashSet<String>();
        ignored.add("的");

        return ignored;
    }
    public static class Map extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private String wordtest = new String();

        // Collection a = { "nihao", "haha" };
        private Set<String> ignored_set = initIgnoredWord();
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();  //得到文件名
            Text fileName_lineOffset = new Text(fileName + "#" + key.toString());
            StringTokenizer itr = new StringTokenizer(value.toString());

            
            for (; itr.hasMoreTokens();) {
                wordtest = itr.nextToken();
                if(ignored_set.contains(wordtest)){
                    continue;
                }
                word.set(wordtest);
                
                // filename_lineoffset: bookname#no
                context.write(word, fileName_lineOffset);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();//values 是一个“键”也就是某一个单词出现的所有位置的记录,
            StringBuilder all = new StringBuilder(); //可看成 values的转化，将values中的记录间加上分割符';'
            if (it.hasNext())
                all.append(it.next().toString());
            for (; it.hasNext();) {
                all.append(";");
                all.append(it.next().toString());
            }
            all.append("\n");
            context.write(key, new Text(all.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "invert index");
        job.setJarByClass(InvertedIndexer.class);

        
        // job.setInputFormatClass(TextInputFormat.class);

        // map
        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // reduce
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
