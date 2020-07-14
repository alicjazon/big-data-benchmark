package databreach

import java.io.File

import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataBreachWriter {

  def main(args: Array[String]) {
    import utils.Schemas._

    val spark = SparkSession
      .builder
      .appName("DataBreach")
      .getOrCreate()

    val t0 = System.nanoTime()

    // VERSION 1 //
//        val websites = getListOfSubDirectories(args(0))
//        val df: DataFrame = websites.map(directory =>
//          spark.read
//            .option("header", value = true)
//            .schema(schema)
//            .csv(s"${args(0)}/$directory/*.csv")
//            .withColumn("database_name", lit(directory)))
//          .reduce(_ union _)
//          .coalesce(4)

    // VERSION 2 //
    val df: DataFrame =
      spark.read
        .option("header", value = true)
        .schema(schema)
        .csv(s"${args(0)}/*.csv") //merged
      //   .csv(s"${args(0)}/*/*.csv")
       //    .withColumn("database_name", regexp_extract(input_file_name(), """([^/]*)/[^/]+\.csv$""", 1))
        .withColumn("database_name", regexp_extract(input_file_name(),
        """[^\\\/]+(?=\.[\w]+$)|[^\\\/]+$""", 0))
        .coalesce(4)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")

    df.select(
      col("email"),
      col("database_name")
    )
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${args(1)}/emails")

    df.select(
      col("password"),
      col("database_name")
    )
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${args(1)}/passwords")

    val t2 = System.nanoTime()
    println("Elapsed time: " + (t2 - t1) + " ns " + ((t2 - t1) / 1000000000) + " s ")
  }

  def getListOfSubDirectories(directoryName: String): Array[String] = {
    new File(directoryName)
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }
}
