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
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.nlpcn.commons.lang.tire.library.Library;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.StringTokenizer;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
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
        extends Mapper<Object, Text, Text, Text> {

        private static Text Key_word = new Text();
        // private static IntWritable Value_int = new IntWritable();
        private static Forest forest = new Forest();

        private static HashSet<String> Name_set = new HashSet<String>();
        InputStream s = null;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            try{
                s = SegWord.class.getClassLoader().getResourceAsStream("library/result.dic");
                forest = Library.makeForest(s);
                // 将姓名表加入hash集合，筛选出分词中的名字，这个setup的作用是最终分词有且仅仅是名字
                Reader reader = new InputStreamReader(SegWord.class.getClassLoader().getResourceAsStream("library/result.dic"));
                BufferedReader in = new BufferedReader(reader);
                String line = null;
                while ((line = in.readLine()) != null) {
                    Name_set.add(line.split("\\s+")[0]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // override map function
        @Override
        protected void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            // FileSplit fileSplit = (FileSplit)content.getInputSplit();
            // Get the filename of files. According to requirements, we need remove the suffix from the filename
            // String fileName = fileSplit.getPath().getName();
            // 调用ansj库对 value 做分词,分词结果可从res中获得
            Result res = DicAnalysis.parse(value.toString(),forest);
            // 每一个 Term 保存一个分词结果
            List<Term> termList = res.getTerms();

            String Name_value = new String();
            for (Term item : termList) {
                // 仅仅选择词性是“人名”的词,nr表示人名
                if (item.getNatureStr().equals("nr") && item.getName().length() > 1
                        && Name_set.contains(item.getName())) {
                    
                    // content.write(Key_word, new Text(item.getName()));
                    Name_value += (item.getName() + " ");
                }
            }
            if (Name_value.length() > 1) {
                // 去掉末尾的空格
                Name_value = Name_value.substring(0, Name_value.length() - 1);
                
                content.write(new Text(key.toString()),new Text(Name_value)); 
                // content.write(new Text(String.valueOf(Name_set.size())),new Text(Name_value)); 

            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private static IntWritable Value_int = new IntWritable(0);
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // values 肯定只有一个值，因为输入文件每一段是唯一的，以锻起始位置相对于文件的偏移量为 key
                context.write(key,value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "segword");
        job.setJarByClass(SegWord.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}