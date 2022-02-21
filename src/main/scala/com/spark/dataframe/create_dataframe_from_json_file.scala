package com.spark.dataframe


import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object create_dataframe_from_json_file {
  def main(args: Array[String]): Unit = {

    println("Apache Spark Application Started .....")

    val spark = SparkSession
      .builder()
      .appName("Create DataFrame from JSON file")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Code Block 1 Start Here

    val json_file_path = "E:\\pract_Dataset\\user_detail.json"

    val user_df_1 = spark.read.json(json_file_path)

    user_df_1.show(10,truncate=false)
    user_df_1.printSchema()


    // Code Block 1 End Here


    // Code Block 2 Start Here
    val json_multiline_file_path = "E:\\pract_Dataset\\user_detail_multiline.json"

    val users_schema = StructType(Array(
      StructField("user_id",IntegerType,true),
      StructField("user_name",StringType,true),
      StructField("user_city",StringType,true))
    )

    //val user_df_2 = spark.read.schema(users_schema).json(json_multiline_file_path)
    val user_df_2 = spark.read.option("multiline","true").json(json_multiline_file_path)

    user_df_2.show(10,truncate=false)
    user_df_2.printSchema()


    // Code Block 2 End Here


    // Code Block 3 Start Here
    val json_multiline_in_list_file_path = "E:\\pract_Dataset\\user_detail_multiline_in_list.json"


    val user_df_3 = spark.read.option("multiline","true").schema(users_schema).json(json_multiline_in_list_file_path)


    user_df_3.show(10,truncate=false)
    user_df_3.printSchema()

    // Code Block 3 End Here





    spark.stop()
    println("Application Completed")

  }

}
