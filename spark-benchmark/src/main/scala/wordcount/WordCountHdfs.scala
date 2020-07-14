package wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountHdfs {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(sparkConf)

    val t0 = System.nanoTime()
    val textFile = sc.textFile("hdfs://localhost:9000/bookMerge.txt")
    val counts = textFile.flatMap(line => line
      .replace("…", "")
      .split("[ ,.+!+?;/:„”—\"%'\\-\\[\\]–)(*]+"))
      .filter(_.length != 0)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
    //  .sortBy(_._2)
    //.sortByKey()
     counts.saveAsTextFile(s"hdfs://localhost:9000/${args(0)}")
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0)/1000000000) + " s ")
  }
}
