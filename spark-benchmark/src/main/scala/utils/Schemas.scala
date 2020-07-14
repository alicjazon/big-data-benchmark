package utils

import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

object Schemas {
  val schema: StructType = new StructType()
    .add("name", StringType, false)
    .add("email", StringType, false)
    .add("phone", StringType, false)
    .add("password", StringType, false)
    .add("card_number", StringType, false)
    .add("address", StringType, false)
    .add("city", StringType, false)
    .add("postal_code", StringType, false)
    .add("country", StringType, false)

  val ecommerce: StructType = new StructType()
    .add("event_time", StringType, false)
    .add("event_type", StringType, false)
    .add("product_id", StringType, false)
    .add("category_id", StringType, false)
    .add("brand", StringType, false)
    .add("price", StringType, false)
    .add("user_id", StringType, false)
    .add("user_session", StringType, false)

  val emailsSchema: StructType = new StructType()
    .add("email", StringType, false)
    .add("database_name", StringType, false)

  val passwordSchema: StructType = new StructType()
    .add("password", StringType, false)
    .add("database_name", StringType, false)

  val emails: String =
    s"""{
       |"table":{"namespace":"default", "name":"emails"},
       |"rowkey":"email",
       |"columns":{
       |"email":{"cf":"rowkey", "col":"email", "type":"string"},
       |"database_name":{"cf":"data", "col":"database", "type":"string"}
       |}
       |}""".stripMargin

  val passwords: String =
    s"""{
       |"table":{"namespace":"default", "name":"passwords"},
       |"rowkey":"password",
       |"columns":{
       |"password":{"cf":"rowkey", "col":"password", "type":"string"},
       |"database_name":{"cf":"data", "col":"database", "type":"string"}
       |}
       |}""".stripMargin
}
