package databreach

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}

object DataBreachWriterCassandra {

  def main(args: Array[String]) {
    import utils.Schemas._

    val spark = SparkSession
      .builder
      .appName("DataBreach")
      .getOrCreate()

    val t0 = System.nanoTime()

    val df =
      spark.read
        .option("header", value = true)
        .schema(schema)
        .csv(s"${args(0)}/*/*.csv")
        .withColumn("database", regexp_extract(input_file_name(), """([^/]*)/[^/]+\.csv$""", 1))
     .coalesce(4)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")

    df.select(
      col("email"),
      col("database")
    )
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "emails", "keyspace" -> "databreach")).save()

    df.select(
      col("password"),
      col("database")
    )
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "passwords", "keyspace" -> "databreach")).save()

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
