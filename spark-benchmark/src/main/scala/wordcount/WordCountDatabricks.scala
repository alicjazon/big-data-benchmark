package wordcount

import config.databricksconfig._
import org.apache.spark.sql.SparkSession

object WordCountDatabricks {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("wordcount")
      .getOrCreate()
    val sc = spark.sparkContext

    spark.sparkContext.hadoopConfiguration.set(
      "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
      storage_account_access_key)

    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line
      .replace("…", "")
      .split("[ ,.+!+?;/:„”—\"%'\\-\\[\\]–)(*]+"))
      .filter(_.length != 0)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)

     counts.saveAsTextFile(args(1))
  }
}
