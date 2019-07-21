package nju
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD 
import scala.collection.immutable.Map
object LPA{
    def main(args:Array[String]):Unit = {
        if(args.length < 4){
            System.err.println("Usage:<inputsFile> <outputFile> <cluster_informathion> <cycle times>")
            System.exit(1)
        }
        // 路径信息配置
        val inputPath:String = args(0)
        val outputPath:String = args(1)
        val clusterPath:String = args(2)
        val cycle_times:Int = args(3).toInt

        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        // 处理节点的聚类信息信息，
 
        val cluster = sc.textFile(clusterPath)
        var cluster_map:Map[String,Int] = cluster.map(line => line.split("\\s+")).map({case Array(a,b) => (a,b.toInt)}).collect.toMap

        // 处理邻接表
        val neig_weight = sc.textFile(inputPath)
        // 将待处理的人物与 与其邻接的节点分开
        var char_neig = neig_weight.map(line => line.split("\\s+"))
        /**对于形如 "吴大鹏  [史松,0.2|汉子,0.025|王潭,0.2|茅十八,0.425|陈近南,0.05|韦小宝,0.075|鳌拜,0.025]" 的一个输入条目
        * solve_label处理方括号内的数据，先将其中的人物名称转换为标签，然后将相同的标签累加(reduce),最终返回权重最大的标签
        */
        def solve_label(neighbor:String) = {
            // var neig:String = neighbor.substring(1,neighbor.length-1)
            var neig_array:Array[String] = neighbor.split(";")
            var neig_tuple:Array[(Int,Double)] = neig_array.map(item => item.split(",")).map({case Array(man,weight) => (cluster_map(man),weight.toDouble)})
            neig_tuple = neig_tuple.groupBy({case (a,b) => a}).values.map(item => item.reduce((a,b) => (a._1,a._2 + b._2))).toArray
            // neig_tuple = sc.makeRDD(neig_tuple).reduceByKey((a,b) => a + b).collect
            neig_tuple.sortBy(r=>r._2).reverse(0)._1
        }
        /** 以下两个write函数用于分别写入两类结果：
         * 1 LPA运行结果 每个条目的形式是 "人物 - 标签"
         * 2 将LPA的结果整理，每个条目变为 "标签 - 人物"
         */
        def write2file(result:RDD[(String,Int)],path:String):Unit={
            result.repartition(1).map({case (a,b) => a + "\t" + b}).saveAsTextFile(path)
        }
        def writeArrange2file(result:RDD[(Int,String)],path:String):Unit={
            result.repartition(1).map({case (a,b) => a.toString + "\t" + b}).saveAsTextFile(path)
        }

        var times = 0
        var char_neig_string:RDD[(String,Int)] = char_neig.map({case Array(a,b,c) => (a,solve_label(c))})
        cluster_map = char_neig_string.collect.toMap
        for(times <- 2 to cycle_times){
            char_neig_string = char_neig.map({case Array(a,b,c) => (a,solve_label(c))})
            cluster_map = char_neig_string.collect.toMap
        }

        var lpaOutputPath = outputPath + "/lpa"
        var arrangeOutputPath = outputPath + "/arrange"
        write2file(char_neig_string,lpaOutputPath)

        // 输出结果整理，这里将对lpa的结果中key-value做转换，输出 "某一标签下有哪些人物" 的形式
        var arrange:RDD[(Int,String)] = char_neig_string.map({case (a,b) => (b,a)}).reduceByKey((a,b) => a + "," + b)

        writeArrange2file(arrange,arrangeOutputPath)
    }
} 