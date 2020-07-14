package wordcount

import org.apache.spark.{SparkConf, SparkContext}
import it.nerdammer.spark.hbase._

object WordCountHbase {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WordCountHbase")
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile("hdfs://localhost:9000/bookMerge.txt")
    val counts = textFile.flatMap(line => line
      .replace("…", "")
      .split("[ ,.+!+?;/:„”—\"%'\\-\\[\\]–)(*]+"))
      .filter(_.length != 0)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)

    val t0 = System.nanoTime()
    counts.toHBaseTable("wordcount")
      .toColumns("word")
      .inColumnFamily("number")
      .save()
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")
  }
}
