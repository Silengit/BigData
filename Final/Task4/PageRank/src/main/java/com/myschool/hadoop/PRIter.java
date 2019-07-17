package com.myschool.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PRIter {

    // 输入格式:姓名   pr值    姓名1,权重1|姓名2,权重2|...
    // 例如:狄云   0.25    戚芳,0.33333|戚长发,0.33333|卜垣,0.33333
    // 输出格式:<key(姓名),key    pr值>(<,>代表这是个键值对)
    // 或者:<key(姓名)  !姓名1,权重1|姓名2,权重2|...>
    // 例如:<戚芳,"狄云 0.08333">或<戚长发,"狄云   0.08333">或<卜垣,"狄云 0.08333">
    // 或者:狄云 !戚芳,0.33333|戚长发,0.33333|卜垣,0.33333
    public static class Map
        extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) 
                    throws IOException, InterruptedException {
            String line = value.toString();
            String[] tuple = line.split("\\s+");
            String nameKey = tuple[0];
            Double pr = Double.parseDouble(tuple[1]);
            if(tuple.length > 2) {
                String[] linkNames = tuple[2].split(";");
                for (String linkName : linkNames) {
                    //pair的形式为:戚芳,0.33333
                    String[] pair = linkName.split(",");
                    Double weight = Double.parseDouble(pair[1]);
                    // Double weight = 1.0;
                    String prValue = nameKey + " " + String.valueOf(pr*weight);
                    //将传递给该linkName的PR值进行输出
                    context.write(new Text(pair[0]), new Text(prValue));
                    // context.write(new Text(linkName), new Text(prValue));
                }
                //传递图结构
                context.write(new Text(nameKey), new Text("!"+tuple[2]));
            }
        }
    }

    // 输入格式:<key(姓名)    <<姓名1,pr值1> <姓名2,pr值2> ...<!姓名1,权重1|姓名2,权重2|...>...>
    // 例如:戚芳    <<"狄云 0.08333"><"!狄云,0.25|戚长发,0.25|卜垣,0.5> <"戚长发 0.08333"> <卜垣 0.125>>
    // 输出格式:<姓名   "pr值   姓名1,权重1|姓名2,权重2|...">  
    // 例如:<戚芳, "0.29167 狄云,0.25|戚长发,0.25|卜垣,0.5">        
    public static class Reduce 
        extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                    throws IOException, InterruptedException {
            String links = "";
            double pr = 0;
            for (Text value : values) {
                String tmp = value.toString();
                if (tmp.startsWith("!")) {
                    links = " " + tmp.substring(tmp.indexOf("!") + 1);
                    continue;
                }
                String[] pair = tmp.split(" ");
                //对pr值进行累加
                pr += Double.parseDouble(pair[1]);
            }
            context.write(new Text(key), new Text(String.valueOf(pr)+links));
        }
    }


    //注意，main函数只运行一次，迭代步骤写在另一个类中
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PRIter.class);

        job.setMapperClass(Map.class);
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
