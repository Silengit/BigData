# 这是任务五：标签传播算法的实现
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
3. cluster: 初始聚类信息
4. cycle_time: 迭代次数

在本任务的输出目录中，将会输出每一次迭代的结果并保存在iterN中，如第3次迭代的结果在iter3中，另外每10次迭代将会运行一次聚类结果的整理，也就是运行Archievement.java中的函数，结果的存放于archievementN中，