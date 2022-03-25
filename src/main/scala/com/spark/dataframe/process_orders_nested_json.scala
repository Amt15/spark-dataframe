package com.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import org.apache.spark.sql.functions._

object process_orders_nested_json extends App {

  println("Apache Spark Application Started .....")

  val spark = SparkSession
    .builder()
    .appName("Process Orders Nested JSON file")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val ordersDf = spark.read.format("json")
  .option("inferSchema", "true")
  .option("multiLine", "true")
  .load("E:\\pract_Dataset\\orders_sample_datasets.json")

  var parseOrdersDf = ordersDf.withColumn("orders", explode(col("datasets")))

  // Step 3: Fetch Each Order using getItem on explode column
  parseOrdersDf = parseOrdersDf.withColumn("customerId", col("orders").getItem("customerId"))
    .withColumn("orderId", col("orders").getItem("orderId"))
    .withColumn("orderDate", col("orders").getItem("orderDate"))
    .withColumn("orderDetails", col("orders").getItem("orderDetails"))
    .withColumn("shipmentDetails", col("orders").getItem("shipmentDetails"))

  parseOrdersDf.show(10,truncate=false)

  parseOrdersDf = parseOrdersDf.withColumn("orderDetails", explode(col("orderDetails")))

  parseOrdersDf = parseOrdersDf.withColumn("productId", col("orderDetails").getItem("productId"))
    .withColumn("quantity", col("orderDetails").getItem("quantity"))
    .withColumn("sequence", col("orderDetails").getItem("sequence"))
    .withColumn("totalPrice", col("orderDetails").getItem("totalPrice"))
    .withColumn("city", col("shipmentDetails").getItem("city"))
    .withColumn("country", col("shipmentDetails").getItem("country"))
    .withColumn("postalcode", col("shipmentDetails").getItem("postalCode"))
    .withColumn("street", col("shipmentDetails").getItem("street"))
    .withColumn("state", col("shipmentDetails").getItem("state"))

  parseOrdersDf.show(10,truncate=false)

  parseOrdersDf = parseOrdersDf.withColumn("gross", col("totalprice").getItem("gross"))
    .withColumn("net", col("totalprice").getItem("net"))
    .withColumn("tax", col("totalprice").getItem("tax"))

  parseOrdersDf.show(10,truncate=false)

  // Step 7: Select required columns from the dataframe
  val jsonParseOrdersDf = parseOrdersDf.select("orderId"
    ,"customerId"
    ,"orderDate"
    ,"productId"
    ,"quantity"
    ,"sequence"
    ,"gross"
    ,"net"
    ,"tax"
    ,"street"
    ,"city"
    ,"state"
    ,"postalcode"
    ,"country")

  jsonParseOrdersDf.show(10,truncate=false)




}
