package databreach

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{col, lit}

object DataBreachFinderHbase {

  def main(args: Array[String]) {
    import utils.Schemas._

    val spark = SparkSession
      .builder
      .appName("DataBreach")
      .getOrCreate()

    def findBreach(parameter: String): Unit = {
      val input = scala.io.StdIn.readLine
      val t1 = System.nanoTime()
      val schema = if (parameter.equals("email")) emails else passwords
      val dataSpreads = spark
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> schema))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
        .filter(col(s"$parameter") === lit(input))
        .select(col("database_name"))
        .collect
        .map(_(0).toString)
        .toList
      if (dataSpreads.isEmpty) println(s"Your $parameter is safe! It is not present in any of leaked user data.")
      else
      { println(s"Your $parameter is present in ${dataSpreads.length} data breaches: ")
        dataSpreads.foreach(println) }
      val t2 = System.nanoTime()
      println("Elapsed time: " + (t2 - t1) + " ns " + ((t2 - t1) / 1000000000) + " s ")
    }
    println("If you want co check email, press 1. If you want to check password, press 2.")
    val option = scala.io.StdIn.readInt
    option match {
      case 1 => findBreach("email")
      case 2 => findBreach("password")
    }

  }

}
