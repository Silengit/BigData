package com.myschool.hadoop;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileTest {
    public static void main(String[] args) throws Exception {
        HashMap<String,String> map = new HashMap<String, String>();
        
        Configuration conf = new Configuration();
        System.out.println("hello world!");
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem result = FileSystem.get(conf);
        FSDataInputStream in = hdfs.open(new Path("cluster.txt"));
        FSDataOutputStream out = result.create(new Path("result.txt"));
        Scanner scan = new Scanner(in);

        while (scan.hasNext()) {
            String str = scan.nextLine();
            // System.out.println(str);
            String tuple[] = str.split("\\s+");
            map.put(tuple[0], tuple[1]);
        }
        scan.close();
        for (String var : map.keySet()) {
            out.write((var + "\t" + 1 + "\n").getBytes());
        }
        in.close();
        out.close();
    }
}