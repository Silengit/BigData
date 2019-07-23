package com.myschool.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SegWordDriver{
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: hadoop jar <*.jar> com.myschool.hadoop.SegWordDriver inputPath outputPath");
        }
        String outputPath = args[1];

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        // 这里分为两种情况，输入路径是一个文件或文件夹，如果是文件夹，那么这个文件夹下的每个文件都有相应的输出文件
        Path inputPath = new Path(args[0]);
        
        if (hdfs.getFileStatus(inputPath).isFile()) {
            // 输入是文件
            SegWord.main(args);
        }
        else {
            FileStatus inputFiles[] = hdfs.listStatus(inputPath);
            for (int i = 0; i < inputFiles.length; i++) {
                if (inputFiles[i].isDirectory()) {
                    //忽略输入目录中的目录
                    continue;
                }
                String inputFilePath = inputFiles[i].getPath().toString();
                String fileName = inputFiles[i].getPath().getName();
                String outputFilePath = outputPath + "/" + fileName.substring(0, fileName.length() - 4);

                System.out.println("正在处理 " + (i + 1) + "/" + inputFiles.length + " ;");

                System.out.println("inputFilePath = " + inputFilePath);
                System.out.println("outputFilePath = " + outputFilePath);

                String localargs[] = { inputFilePath, outputFilePath };
                SegWord.main(localargs);
            }
        }
    }
}