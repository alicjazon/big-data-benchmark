package databreach

import java.io.File

import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataBreachStreaming {

  def main(args: Array[String]) {
    import utils.Schemas._

    val spark = SparkSession
      .builder
      .appName("DataBreach")
      .getOrCreate()

    val t0 = System.nanoTime()

    val df: DataFrame =
      spark.readStream
        .option("header", value = true)
        .schema(schema)
        .csv(s"${args(0)}/userDataStream/*.csv")
        .withColumn("database_name", regexp_extract(input_file_name(),
        """[^\\\/]+(?=\.[\w]+$)|[^\\\/]+$""", 0))
        .coalesce(1)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")

    df.select(
      col("email"),
      col("database_name")
    )
      .writeStream
      .format("parquet")
      .option("path", s"${args(1)}/emails")
      .option("checkpointLocation", "src/main/resources/checkpoint")
      .outputMode(OutputMode.Append())
      .start
      .awaitTermination

    df.select(
      col("password"),
      col("database_name")
    )
      .writeStream
      .format("parquet")
      .option("path", s"${args(1)}/passwords")
      .option("checkpointLocation", "src/main/resources/checkpoint")
      .outputMode(OutputMode.Append())
      .start
      .awaitTermination

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
