package databreach

import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataBreachWriterHdfs {

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
        .csv(s"hdfs://localhost:9000/userDataMerged/*.csv")
        .withColumn("database_name", regexp_extract(input_file_name(),
          """[^\\\/]+(?=\.[\w]+$)|[^\\\/]+$""", 0))
        //    .csv(s"hdfs://localhost:9000/userData/*/*.csv")
        //      .withColumn("database_name", regexp_extract(input_file_name(), """([^/]*)/[^/]+\.csv$""", 1))
        .coalesce(4)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + " ns " + ((t1 - t0) / 1000000000) + " s ")
    //
    df.select(
      col("email"),
      col("database_name")
    )
      .write
      .mode(SaveMode.Overwrite)
      .csv("hdfs://localhost:9000/emails")
//      .options(
//      Map(HBaseTableCatalog.tableCatalog -> emails, HBaseTableCatalog.newTable -> "5"))
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()

    df.select(
      col("password"),
      col("database_name")
    )
      .write
      .mode(SaveMode.Overwrite)
      .csv("hdfs://localhost:9000/passwords")
//      .options(
//        Map(HBaseTableCatalog.tableCatalog -> passwords, HBaseTableCatalog.newTable -> "5"))
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()

    val t2 = System.nanoTime()
    println("Elapsed time: " + (t2 - t1) + " ns " + ((t2 - t1) / 1000000000) + " s ")
  }
}
