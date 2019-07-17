# 这是任务一：分词算法的实现
**源代码的内容如下**
1. SegWord.java: 对单个文件的分词
2. SegWordDriver.java: 配置文件路径，通过调用“SegWord”完成多个文件的单独分词

**运行方法**  
1. 编译命令  
进入项目目录:
```shell
mvn clean package
```
2. hadoop上的执行命令  

```shell
hadoop jar hadoop-1.0-SNAPSHOT.jar com.myschool.hadoop.SegWordDriver <inputPath> <outputPath>
```
下面是参数的解释:
1. inputPath: 未分词过的金庸小说
2. outputPath: 本次任务的输出路径

