# 程序运行方式

## 任务4 PageRank

1. 在进程中登录集群并将当前目录切换至Final

2. 输入指令

```shell
hadoop jar package/PRDriver.jar com.myschool.hadoop.PRDriver Task3output_v3.0 Task4output 10
```

其中最后的数字表示迭代次数。注意，需保证hdfs中没有Task4output和Task4outputFinalRank目录。

3. 使用指令

```shell
hdfs dfs -cat Task4outputFinalRank/part-r-00000
```

来查看运行结果。

## 附加任务 spark（Task2-4）

1. 在本地进入BigData目录。

2. 输入指令

```shell
~/Workspace/spark-2.4.3-bin-hadoop2.7/bin/spark-submit --class "WCPR" --master local[4] Final\(scala\)/target/scala-2.11/wcpr_2.11-1.0.jar 10
```

其中最后的数字表示迭代次数。

3. 输出文件output.txt在Final\(scala\)/resource目录下。