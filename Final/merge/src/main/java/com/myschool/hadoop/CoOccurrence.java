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
import java.util.Vector;

public class CoOccurrence {

    // map class extend Mapper
    public static class Map
        extends Mapper<Object, Text, Text, IntWritable> {

        private static Text Key_word = new Text();
        private static IntWritable Value_int = new IntWritable();

        // override map function
        @Override
        protected void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            Vector people = new Vector();
            StringTokenizer itr = new StringTokenizer(value.toString());
            // remove the column number
            int colflag = 0;
            while(itr.hasMoreTokens()){
                String word = itr.nextToken();
                if(colflag == 0) {
                    colflag = 1;
                    continue;
                }
                if(!people.contains(word))
                    people.addElement(word);
            }

            Value_int.set(1);
            for(int i=0;i<people.size()-1;i+=1) {
                for(int j=i+1;j<people.size();j+=1) {
                    if(!people.elementAt(i).toString().equals(people.elementAt(j).toString())) {
                        Key_word.set(people.elementAt(i).toString() + ',' + people.elementAt(j).toString());
                        content.write(Key_word, Value_int);
                        Key_word.set(people.elementAt(j).toString() + ',' + people.elementAt(i).toString());
                        content.write(Key_word, Value_int);
                    }
                }
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

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // split word and filename
            String[] source = key.toString().split(",");
            String fp = source[0];
            String sp = source[1];

            int frequency = 0;
            for(IntWritable value : values) {
                frequency += value.get();
            }
            Key_word.set(fp);
            textValue.set(sp + ' ' + frequency);
            context.write(Key_word, textValue);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "People Co-occurrence");
        job.setJarByClass(CoOccurrence.class);

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
