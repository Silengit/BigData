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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.List;
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

public class InvertedIndexer {

    // map class extend Mapper
    public static class Map
        extends Mapper<Object, Text, Text, IntWritable> {

        private static Text Key_word = new Text();
        private static IntWritable Value_int = new IntWritable();

        // override map function
        @Override
        protected void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            // this ignore class can judge a element(String) wether or not be filter
            Ignore ignore = new Ignore();

            FileSplit fileSplit = (FileSplit)content.getInputSplit();
            // Get the filename of files. According to requirements, we need remove the suffix from the filename
            String fileName = fileSplit.getPath().getName();
            if(fileName.length() > 14) {
                fileName = fileName.substring(0, fileName.length()-14);
            }

            //Count the frequency of words that appear in a line.
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String word = itr.nextToken();
                if(ignore.contains(word)){  //  if an element is in ignore_set,it will be ignored,
                    continue;
                }

                Key_word.set(word+","+fileName);
                Value_int.set(1);
                content.write(Key_word, Value_int);
            }
        }

        static class WordCount {
            private String word;
            private int count;

            public WordCount() {
                super();
            }

            public String getWord() {
                return word;
            }
            public void setWord(String Word) {
                this.word = Word;
            }

            public int getCount() {
                return count;
            }
            public void setCount(int Count) {
                this.count = Count;
            }
        }
    }

    // combine class extend Reducer, cause we need to combine same word in same file, and let them as one key
    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable Value_int = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

            // Totally number of the word appear in a specific file
            //Value_int.set(0);
            int sum = 0;

            for(IntWritable value : values)
                sum += value.get();
                //Value_int.set(Value_int.get() + value.get());

            Value_int.set(sum);
            context.write(key, Value_int);
        }
    }

    // Change the key of patition, use default patition function
    public static class WordPartition extends HashPartitioner<Text, Object> {

        private static Text keyword = new Text();
        @Override
        public int getPartition(Text key, Object value, int numReduceTasks) {

            keyword.set(key.toString().split(",")[0]);

            return super.getPartition(keyword, value, numReduceTasks);
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

            // when one word's calculate is end, calculate the average appear number.
            if(curWord != null && (word.equals(curWord) == false)){
                Key_word.set(curWord);
                float average = (float) appear_num / file_num ;
                textValue.set(average+","+ValueText.toString());
                context.write(Key_word, textValue);

                file_num = 0;
                appear_num = 0;
                ValueText.delete(0, ValueText.length());
            }

            curWord = word;

            int frequency = 0;
            for(IntWritable value : values)
                frequency += value.get();
                file_num += 1;
            appear_num += frequency;

            if(file_num > 1) ValueText.append(";");

            ValueText.append(filename+":"+frequency);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndexer.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(Combiner.class);
        job.setPartitionerClass(WordPartition.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
