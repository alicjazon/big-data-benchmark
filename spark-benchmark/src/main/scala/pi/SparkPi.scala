package pi

import org.apache.spark.sql.SparkSession

import scala.math.random

object SparkPi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = if (args.length > 1) args(1).toInt else 100
    val t0 = System.nanoTime()
    val count = spark.sparkContext.parallelize(1 until n, slices)
    val pi = count.map { _ =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"*****Pi is ${4.0 * pi / (n - 1)}*****")
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns")
  }
}
