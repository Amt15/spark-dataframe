package com.spark.dataframe

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.databricks.spark.xml._

object create_dataframe_from_xml_file  {
  def main(args: Array[String]): Unit = {

    println("Apache Spark Application Started .....")

    val spark = SparkSession
      .builder()
      .appName("Create DataFrame from XML file")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Code Block 1 Starts Here
    println("Approach 1: ")
    val xml_file_path = "E:\\pract_Dataset\\user_detail.xml"
    val users_df_1 = spark.read.option("rowTag","user").xml(xml_file_path)

    users_df_1.show(10,truncate=false)
    users_df_1.printSchema()

    // Code Block 1 Ends Here

    println("Approach 2: ")
    val users_schema = StructType(Array(
      StructField("user_id",IntegerType,true),
      StructField("user_name",StringType,true),
      StructField("user_city",StringType,true))
    )

    val users_df_2 = spark.read.schema(users_schema).option("rowTag","user").xml(xml_file_path)

    users_df_2.show(10,truncate=false)
    users_df_2.printSchema()

    // Code Block 2 Ends Here

    spark.stop()
    println("Apache Spark Applications Completed")



  }



}
