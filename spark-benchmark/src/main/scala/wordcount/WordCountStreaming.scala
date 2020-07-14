package wordcount

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCountStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WordCountStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val t0 = System.nanoTime()
    val textFile = ssc.textFileStream(args(0))
    val counts = textFile.flatMap(line => line
      .replace("…", "")
      .split("[ ,.+!+?;/:„”—\"%'\\-\\[\\]–)(*]+"))
      .filter(_.length != 0)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)

    counts.saveAsTextFiles("out")
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")
    ssc.start()
    ssc.awaitTermination()
  }
}
