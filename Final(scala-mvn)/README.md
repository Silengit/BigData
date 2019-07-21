# 任务五LPA算法的spark实现
**源代码的内容如下**
1. LPA.scala: 包含了lpa和lpa结果整理的代码

**运行方法**  
1. 编译命令  
进入项目目录:
```shell
mvn clean package
```
2. spark上的执行命令  

```shell
spark-submit --class nju.LPA cyd-1.0-SNAPSHOT.jar <inputPath> <outputPath> <cluster> <cycle_time>
```
下面是参数的解释:
1. inputPath: 任务三的输出结果
2. outputPath: 本次任务的输出路径
3. cluster: 初始聚类信息,需要将hadoop版本的Lpa项目的resources下的result.txt上传到集群上，并将其路径作为cluster
4. cycle_time: 迭代次数