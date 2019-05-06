package hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;

public class App {
    private static Configuration conf = null;
    static {
        // 手动指定hadoop home的路径
        System.setProperty("hadoop.home.dir", "/home/hadoop/hadoop_installs/hadoop-2.9.2/");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static void main(String[] args) {
        try {
            String tableName = "Wuxia";   //数据库 表明
            String outFileName = "wuxia_output.txt";   //输出文件名
            // 遍历 tableName 这个表并将内容 写入 文件
            scanRows(tableName,outFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // scan table rows
    public static void scanRows(String tableName,String outFileName) throws IOException {
        // 指定输出文件
        File outputfile = new File(outFileName);
        if (!outputfile.exists()) {
            try {
                outputfile.createNewFile();
            } catch (IOException e) {
                    // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        // 将内容输出到 文件中
        Writer out = new FileWriter(outputfile);

        //确定 要访问的表明
        HTable table = new HTable(conf, tableName);
        //指定scan 的区域
        Scan s = new Scan();
        String frequency = new String();
        String word = new String();
        ResultScanner ss = table.getScanner(s);
        // 打印扫描结果
        for (Result r : ss) {
            for (KeyValue kv : r.raw()) {
                if (new String(kv.getQualifier()).equals("Avefrequency")) {
                    frequency = new String(kv.getValue()); //获取 词语 平均出现次数
                } else if (new String(kv.getQualifier()).equals("Word")) {
                    word = new String(kv.getValue()); //获取 词语
                }
            }
            String result = word + '\t' + frequency;
            out.write(result+'\n');
        }
        out.close();
        System.out.println("读取数据库 " + tableName + " 并写入 " + outFileName + " 已完成");
    }
}
