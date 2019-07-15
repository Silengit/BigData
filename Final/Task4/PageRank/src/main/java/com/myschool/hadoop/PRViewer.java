package com.myschool.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


//对pr进行排序
public class PRViewer {

    private static Text outName = new Text();
    private static DoubleWritable outPr = new DoubleWritable();
    public static class Map
        extends Mapper<Object, Text, DoubleWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) 
                    throws IOException, InterruptedException {
            String line = value.toString();
            String[] tuple = line.split("\\s+");
            String nameKey = tuple[0];
            Double pr = Double.parseDouble(tuple[1]);
            outName.set(nameKey); 
            outPr.set(pr);
            context.write(outPr, outName);
            }
        }
    
    public static class DoubleWritableDecreasingComparator
        extends DoubleWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1,
            byte[] b2, int s2, int l2) {
            double thisValue = readDouble(b1, s1);
            double thatValue = readDouble(b2, s2);
            return -(thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }
    
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PRViewer.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setSortComparatorClass(DoubleWritableDecreasingComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);
    }
}
