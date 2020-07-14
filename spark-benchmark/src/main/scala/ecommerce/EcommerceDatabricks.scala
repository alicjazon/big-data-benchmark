package ecommerce

import config.databricksconfig.storage_account_access_key
import org.apache.spark.sql.SparkSession
import config.databricksconfig._

object EcommerceDatabricks {
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

//    val event_time = ecommerceRDD.map(p => p(0))
//    val event_type = ecommerceRDD.map(p => p(1))
//    val product_id = ecommerceRDD.map(p => p(2))
//    val category_id = ecommerceRDD.map(p => p(3))
//    val category_code = ecommerceRDD.map(p => p(4))
//    val brand = ecommerceRDD.map(p => p(5))
//    val price = ecommerceRDD.map(p => p(6))
//    val user_id = ecommerceRDD.map(p => p(7))
//    val user_session = ecommerceRDD.map(p => p(8))

    ecommerceRDD
      .map(p =>
        (if (p(4).split("\\.")(0) == "") p(5) else
          p(4).split("\\.")(0).toUpperCase,
          if (p(6) == "") 0.0 else p(6).toDouble))
      .filter(_._1.length != 0)
      .reduceByKey(_ + _)
      .sortByKey()
      .saveAsTextFile(args(1))
  }
}
