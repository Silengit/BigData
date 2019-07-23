# 程序运行方式
## 任务1 分词算法的实现
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

## 任务2 任务同现统计的实现
**源代码的内容如下**
1. CoOccurrence.java: 对人物分词后的结果，统计两个人物在同一段中出现的次数

**运行方法**  
1. 编译命令  
进入项目目录:
```shell
mvn clean package
```
2. hadoop上的执行命令  

```shell
hadoop jar hadoop-1.0-SNAPSHOT.jar com.myschool.hadoop.CoOccurrence <inputPath> <outputPath>
```
下面是参数的解释:
1. inputPath: 任务1阶段的输出文件
2. outputPath: 本次任务的输出路径

## 任务3 人物关系图构建与特征归一化
**源代码的内容如下**
1. Normalization.java: 对人物同现次数统计后的结果，输出小说集中的每个人物及与其有关的人物，并给出两两人物之间边的权重

**运行方法**  
1. 编译命令  
进入项目目录:
```shell
mvn clean package
```
2. hadoop上的执行命令  

```shell
hadoop jar hadoop-1.0-SNAPSHOT.jar com.myschool.hadoop.Normalization <inputPath> <outputPath>
```
下面是参数的解释:
1. inputPath: 任务2阶段的输出文件
2. outputPath: 本次任务的输出路径

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

## 任务五：标签传播算法的实现
**源代码的内容如下**
1. LabelPropagation.java: lpa算法的单次迭代实现
2. LPADriver.java: 配置文件路径，通过调用“单次迭代的实现”完成多次迭代
3. Archievement.java:lpa运行结果的整理，把原来"人物-标签”的键值关系变为"标签-人物“，这样一个”标签”会有多个“人物”

**运行方法**  
1. 编译命令  
进入项目目录:
```shell
mvn clean package
```
2. hadoop上的执行命令  

```shell
hadoop jar hadoop-1.0-SNAPSHOT.jar com.myschool.hadoop.LPADriver <inputPath> <outputPath> <cluster> <cycle_times>
```
如上，需要使用LPADriver来实现多次迭代，后面接4个参数，下面它们的解释:
1. inputPath: 任务三的输出结果
2. outputPath: 本次任务的输出路径
3. cluster: 初始聚类信息,需要将本项目的resources下的result.txt上传到集群上，并将其路径作为cluster
4. cycle_time: 迭代次数

输入指令例如: hadoop jar hadoop-1.0-SNAPSHOT.jar com.myschool.hadoop.LPADriver novels output cluster.txt 100  
在本任务的输出目录中，将会输出每一次迭代的结果并保存在iterN中，如第3次迭代的结果在iter3中，另外每10次迭代将会运行一次聚类结果的整理，也就是运行Archievement.java中的函数，结果的存放于archievementN中，
## 附加任务 spark（Task2-4）

1. 在本地进入BigData目录。

2. 输入指令

```shell
"your spark directory"/bin/spark-submit --class "WCPR" --master local[4] Final\(scala\)/target/scala-2.11/wcpr_2.11-1.0.jar 10
```

其中最后的数字表示迭代次数。

3. 输出文件output.txt在Final\(scala\)/resource目录下。
## 附加任务 spark (Task5)
**源代码的内容如下**
1. LPA.scala: 包含了lpa和lpa结果整理的代码

**运行方法**  
1. 编译命令  
进入项目目录(即Final(scala-mvn)/)后:
```shell
mvn clean package
```
2. spark上的执行命令  

```shell
spark-submit --class nju.LPA cyd-1.0-SNAPSHOT.jar <inputPath> <outputPath> <cluster> <cycle_time>
```
下面是参数的解释(和mapreduce的lpa实现相同):
1. inputPath: 任务三的输出结果
2. outputPath: 本次任务的输出路径
3. cluster: 初始聚类信息,需要将hadoop版本的Lpa项目的resources下的result.txt上传到集群上，并将其路径作为cluster
4. cycle_time: 迭代次数

# Gephi图展示
- 目录下graph.gephi是Gephi图文件，效果图展示在报告中有给出。
- 目录下Gephi展示.wmv展示了人物图的构建及收敛全过程。
