package wordcount

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDataFrame {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder.getOrCreate

    import spark.implicits._

    val t0 = System.nanoTime()
    val textFile: DataFrame = sc.textFile(args(0)).toDF("line")

    val counts: DataFrame = textFile
      .withColumn("word", explode(split($"line", "[ ,.+!+?;/:„”—\"%'\\-\\[\\]–)(*]+")))
      .withColumn("wordCleaned", when(col("word") === lit("…"), "")
        .otherwise(col("word")))
      .select(lower(col("wordCleaned")))
      .filter(not(col("wordCleaned") === lit("")))
    val c = counts
      .groupBy("wordCleaned").count
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(args(1))

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")
  }
}
