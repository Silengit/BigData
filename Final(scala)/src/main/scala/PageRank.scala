import org.apache.spark.sql.SparkSession
import java.io._

object PageRank {
    var weight = new Array[(String, Array[(String, Double)])](100)
    var n = 0
    var times = 10

    def readFromTxt(s:SparkSession, f:String): Array[(String, Array[(String, Double)])] = {
        val line = s.read.textFile(f).collect
        val keyname = line.map(x => x.split("\\s+")(0))
        val link = line.map(x => x.split("\\s+")(2)).map(x => x.split(";"))
        val n = keyname.length
        var ret = new Array[(String, Array[(String, Double)])](n)
        for(i <- 0 until n) {
            val m = link(i).length
            var l = new Array[(String, Double)](m)
            for(j <- 0 until m) {
                val tmp = link(i)(j).split(",")
                l(j) = (tmp(0),tmp(1).toDouble)
            }
            ret(i) = (keyname(i), l)
        }
        return ret
    }

    def write2Txt(pr:Array[Double]) {
        val writer = new PrintWriter(new File("Final(scala)/resource/output3.txt"))
        var pair = new Array[(Double, String)](n)
        for( i <- 0 until n) 
            pair(i) = (pr(i), weight(i)._1)   
        val result = pair.sortBy(r => r._1)(Ordering.Double.reverse)
        for( i <- 0 until n) 
            writer.write(result(i)._1+"\n")
        writer.close()   
    }

    def name2Idx(b:String): Int = {
        for( i <- 0 until n )
            if( weight(i)._1 == b )
                return i
        return 0
    }

    def weightOf(a:String, b:Int): Double = {
        val idx = name2Idx(a)
        val m = weight(idx)._2.length
        for( i <- 0 until m ) {
            // println(weight(idx)._2(i)._1+"\t"+weight(b)._1)
            if( weight(idx)._2(i)._1 == weight(b)._1 )
                return weight(idx)._2(i)._2
        }
        return 1.0
    }

    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("PageRank").getOrCreate()
        weight = readFromTxt(spark, "Final(scala)/resource/input3.txt")
        n = weight.length
        times = args(0).toInt
        var pr = Array.fill(n)( 1.0 )
        for( iter <- 0 until times ) {
            var tmp = Array.fill(n)( 1.0 )
            val oldPR = pr
            for( i <- 0 until n ) 
                tmp(i) = weight(i)._2.map(j => oldPR(name2Idx(j._1))*weightOf(j._1,i)).sum
            pr = tmp
        }
        write2Txt(pr)
        spark.stop()
    }
}