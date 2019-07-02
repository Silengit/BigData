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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Vector;
import java.util.List;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.library.DicLibrary;
import org.ansj.app.keyword.Keyword;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
public class SegWord {

    // map class extend Mapper
    public static class Map
        extends Mapper<Object, Text, Text, IntWritable> {

        private static Text Key_word = new Text();
        private static IntWritable Value_int = new IntWritable();
        
        // override map function
        @Override
        protected void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)content.getInputSplit();
            // Get the filename of files. According to requirements, we need remove the suffix from the filename
            String fileName = fileSplit.getPath().getName();

            // 调用ansj库对 value 做分词,分词结果可从res中获得
            Result res = DicAnalysis.parse(value.toString());
            // 每一个 Term 保存一个分词结果
            List<Term> termList = res.getTerms();

            // Key_word.set(key.toString());
            String wordkey = null;
            for (Term item : termList) {
                // 仅仅选择词性是“人名”的词,nr表示人名
                if (item.getNatureStr().equals("nr")) {
                    // content.write(Key_word, new Text(item.getName()));
                    wordkey += (item.getName() + " ");
                }
            }
            wordkey = wordkey.subSequence(0, wordkey.length() - 1).toString();
            Value_int.set(Integer.parseInt(key.toString()));
            Key_word.set(wordkey);
            content.write(Key_word, Value_int);
            // while(itr.hasMoreTokens()){
            //     String word = itr.nextToken();
            //     // if(ignore.contains(word)){  //  if an element is in ignore_set,it will be ignored,
            //     //     continue;
            //     // }

            //     Key_word.set(word+","+fileName);
            //     Value_int.set(1);
            //     content.write(Key_word, Value_int);
            // }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private static IntWritable Value_int = new IntWritable(0);

        private static Text Key_word = new Text();
        StringBuffer ValueText = new StringBuffer();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // split word and filename
            // String[] source = key.toString().split(",");
            // String word = source[0];
            // String filename = source[1];
            // when one word's calculate is end, calculate the average appear number.
            Text colnum = new Text();
            for (IntWritable value : values) {
                colnum.set(value.toString());
            }
            context.write(colnum, key);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "segword");
        job.setJarByClass(SegWord.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}