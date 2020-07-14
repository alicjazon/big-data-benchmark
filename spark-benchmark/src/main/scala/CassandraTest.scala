import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession

object CassandraTest extends App{
    val spark = SparkSession
      .builder
      .appName("Cassandra test")
      .getOrCreate()
    val rdd = spark.sparkContext.cassandraTable("test", "kv")
    println(rdd.count)
    println(rdd.first)
    println(rdd.map(_.getInt("value")).sum)
    val collection = spark.sparkContext.parallelize(Seq(("key3", 3), ("key4", 4)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
}
