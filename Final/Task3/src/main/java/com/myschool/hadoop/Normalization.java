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

public class Normalization {

    // map class extend Mapper
    public static class Map
        extends Mapper<Object, Text, Text, IntWritable> {

        private static Text Key_word = new Text();
        private static IntWritable Value_int = new IntWritable();

        // override map function
        @Override
        protected void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int count = 0;
            String fp = null;
            String sp = null;
            int conum = 0;
            while(itr.hasMoreTokens()){
                if(count == 0)
                    fp = itr.nextToken();
                else if(count == 1)
                    sp = itr.nextToken();
                else if(count == 2)
                    conum = Integer.parseInt(itr.nextToken());
                count += 1;
            }
            Key_word.set(fp + ',' + sp);
            Value_int.set(conum);
            content.write(Key_word, Value_int);
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

        private static String curPeople = null;
        private static int appear_num = 0;
        private static int co_num = 0;
        private static Vector relative_people = new Vector();
        private static Vector relative_frequency = new Vector();
        private static double init_PR = 1.0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // split word and filename
            String[] source = key.toString().split(",");
            String fp = source[0];
            String sp = source[1];

            if(curPeople != null && (fp.equals(curPeople) == false)){
                StringBuffer ValueText = new StringBuffer();
                Key_word.set(curPeople);
                //ValueText.append("[");
                //ValueText.append(init_PR);
                //ValueText.append("\t");
                for(int i = 0; i < co_num; i += 1) {
                    if(i > 0) ValueText.append(";");
                    ValueText.append(relative_people.elementAt(i).toString() + ',');
                    float cur_num = Integer.parseInt(relative_frequency.elementAt(i).toString());
                    //cur_num /= appear_num;
                    //ValueText.append(cur_num);
                    ValueText.append(cur_num);
                }
                //ValueText.append("]");
                textValue.set(ValueText.toString());
                context.write(Key_word, textValue);

                co_num = 0;
                appear_num = 0;
                ValueText.delete(0, ValueText.length());
                relative_people.clear();
                relative_frequency.clear();
            }

            curPeople = fp;

            int frequency = 0;
            for(IntWritable value : values) {
                frequency += value.get();
            }
            appear_num += frequency;

            relative_people.addElement(sp);
            relative_frequency.addElement(frequency);

            co_num += 1;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuffer ValueText = new StringBuffer();
            Key_word.set(curPeople);
            //ValueText.append("[");
            //ValueText.append(init_PR);
            //ValueText.append("\t");
            for(int i = 0; i < co_num; i += 1) {
                if(i > 0) ValueText.append(";");
                ValueText.append(relative_people.elementAt(i).toString() + ',');
                float cur_num = Integer.parseInt(relative_frequency.elementAt(i).toString());
                //cur_num /= appear_num;
                //ValueText.append(cur_num);
                ValueText.append(cur_num);
            }
            //ValueText.append("]");
            textValue.set(ValueText.toString());
            context.write(Key_word, textValue);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Normalization Score");
        job.setJarByClass(Normalization.class);

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
