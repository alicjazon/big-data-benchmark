package wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(sparkConf)

    val t0 = System.nanoTime()
    val textFile = sc.textFile(args(0))
    val counts: RDD[(String, Int)] = textFile.flatMap(line => line
      .replace("…", "")
      .split("[ ,.+!+?;/:„”—\"%'\\-\\[\\]–)(*]+"))
      .filter(_.length != 0)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")
  }
}
