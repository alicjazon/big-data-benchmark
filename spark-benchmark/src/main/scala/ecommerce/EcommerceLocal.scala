package ecommerce

import org.apache.spark.sql.SparkSession

object EcommerceLocal {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Ecommerce")
      .getOrCreate()
    val sc = spark.sparkContext

    val textFile = sc.textFile(s"${args(0)}/Nov-2019")
    val textFile2 = sc.textFile(s"${args(0)}/Oct-2019")

    val ecommerdeRDD = textFile.union(textFile2)
      .map(_.split(","))
      .map(p =>
        (if (p(4).split("\\.")(0) == "") p(5) else
          p(4).split("\\.")(0).toUpperCase,
          if (p(6) == "") 0.0 else p(6).toDouble))
      .filter(_._1.length != 0)
      .reduceByKey(_ + _)
      .sortByKey()

    ecommerdeRDD.saveAsTextFile(s"${args(1)}/ecommout")
  }
}
