package ecommerce

import config.databricksconfig.storage_account_access_key
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SaveMode, SparkSession}
import config.databricksconfig._

object EcommerceTableDatabricks {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Ecommerce")
      .getOrCreate()

    spark.conf.set(
      "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net",
      storage_account_access_key)

    val df = spark.read.table("ecomm_small")

    val event_time = df.select("_c0")
//    val event_type = df.select("_c1")
//    val product_id = df.select("_c2")
//    val category_id = df.select("_c3")
//    val category_code = df.select("_c4")
//    val brand = df.select("_c5")
//    val price = df.select("_c6")
//    val user_id = df.select("_c7")
//    val user_session = df.select("_c8")

    df
      .withColumn("key", when(split(col("_c4"), "\\.")(0).isNull, col("_c5"))
        .otherwise(upper(split(col("_c4"), "\\.")(0))))
      .withColumn("price", when(col("_c6") === "", 0.0)
        .otherwise(col("_c6").cast(DoubleType)))
      .groupBy("key")
      .agg(sum("price").as("value"))
      .orderBy("key")
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ecomm_out")
  }
}
