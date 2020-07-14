package wordcount

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object CassandraWordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line
      .replace("…", "")
      .split("[ ,.+!+?;/:„”—\"%'\\-\\[\\]–)(*]+"))
      .filter(_.length != 0)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)

    val t0 = System.nanoTime()
    counts.saveToCassandra("test", "wordcount", SomeColumns("key", "value"))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")
  }
}
