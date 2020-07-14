package ecommerce

import config.databricksconfig.storage_account_access_key
import org.apache.spark.sql.SparkSession
import config.databricksconfig._

object EcommercePerformanceBench {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Ecommerce")
      .getOrCreate()
    val sc = spark.sparkContext

val file_location = args(0)
    spark.sparkContext.hadoopConfiguration.set(
      "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
      storage_account_access_key)

    val textFile = sc.textFile(file_location)

    val ecommerceRDD = textFile
      .map(_.split(","))

    val sum2 = (1 to 20000).sum
    val sum = (1 to 10000).sum

    ecommerceRDD
      .map(p =>
        (if (p(4).split("\\.")(0) == "") p(5) else
          p(4).split("\\.")(0).toUpperCase,
          if (p(6) == "") 0.0 else sum/sum2))
      .filter(_._1.length != 0)
      .reduceByKey(_ + _)
      .sortByKey()
      .saveAsTextFile(args(1))
  }
}
