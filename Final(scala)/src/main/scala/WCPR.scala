import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import java.io._
import scala.collection.mutable
object WCPR {

    val warray = new mutable.ArrayBuffer[(String, Array[(String, Double)])]()
    var n = 0

    def name2Idx(b:String): Int = {
        for( i <- 0 until n )
            if( warray(i)._1 == b )
                return i
        return 0
    }

    def weightOf(a:String, b:Int): Double = {
        val idx = name2Idx(a)
        val m = warray(idx)._2.length
        for( i <- 0 until m ) {
            if( warray(idx)._2(i)._1 == warray(b)._1 )
                return warray(idx)._2(i)._2
        }
        return 1.0
    }

    def write2Txt(pr:Array[Double]) {
        val writer = new PrintWriter(new File("Final(scala)/resource/output.txt"))
        var pair = new Array[(Double, String)](n)
        for( i <- 0 until n) 
            pair(i) = (pr(i), warray(i)._1)   
        val result = pair.sortBy(r => r._1)(Ordering.Double.reverse)
        for( i <- 0 until n) 
            writer.write(result(i)._1 + "\t" + result(i)._2 + "\n")
        writer.close()   
    }

    def main(args: Array[String]) {

        //**********word counting**********
        val path = "Final(scala)/resource/input1.txt"
        val spark = SparkSession.builder.appName("WCPR").getOrCreate()
        val sc = SparkContext.getOrCreate()
        val words = spark.read.textFile(path).collect.map(line => line.split("\\s+").distinct)
        val coTerm = words.map { line =>
            for{
                i <- 1 until line.length
                j <- (i+1) until line.length
            } yield {
                (line(i), line(j))
            } 
        }.flatMap(x => x).flatMap(x => List(x,(x._2,x._1)))
        val rdd = sc.parallelize(coTerm)
        val results = rdd.map(w => (w,1)).reduceByKey({case(x,y) => x+y})
        // results.coalesce(1).saveAsTextFile("scala_project/resource/output1")

        //**********graph building**********
        val sum = results.map(w => (w._1._1,w._2)).reduceByKey({case(x,y) => x+y}).collectAsMap()
        val weight = results.map(w => (w._1, w._2.toDouble/sum(w._1._1))).map(
            w => (w._1._1,(w._1._2,w._2))).groupByKey()
        //weight.coalesce(1).saveAsTextFile("scala_project/resource/output2")

        //**********page ranking**********
        //RDD => Array
        weight.foreach(r => {
            var tmp = new Array[(String, Double)](r._2.size)
            var idx = 0
            for(i <- r._2) {
                tmp(idx) = i
                idx += 1
            }
            val pair = (r._1, tmp)
            warray += pair
        })

        //Initialize PageRank
        n = warray.length
        var pr = Array.fill(n)( 1.0 )

        //Iterate computing PageRank
        for( iter <- 0 until 50 ) {
            var tmp = Array.fill(n)( 1.0 )
            val oldPR = pr
            for( i <- 0 until n ) 
                tmp(i) = warray(i)._2.map(j => oldPR(name2Idx(j._1))*weightOf(j._1,i)).sum
            pr = tmp
        }
        write2Txt(pr)
        spark.stop()
    }
}