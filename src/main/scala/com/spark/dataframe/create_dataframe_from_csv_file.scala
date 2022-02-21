package com.spark.dataframe

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object create_dataframe_from_csv_file extends App {

  println("Apache Spark Application Started ....")

  val spark = SparkSession
    .builder()
    .appName("Create DataFrame from CSV file")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val csv_comma_delimiter_file_path = "E:\\pract_Dataset\\user_detail_comma_delimiter.csv"

  //val users_df_1 = spark.read.csv(csv_comma_delimiter_file_path)
  //val users_df_1 = spark.read.option("header",true).csv(csv_comma_delimiter_file_path)

  val users_df_1 = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv(csv_comma_delimiter_file_path)

  users_df_1.show(6,truncate=false)

  users_df_1.printSchema()

  val user_schema = StructType(Array(
    StructField("emp_id",IntegerType,true),
    StructField("emp_name",StringType,true),
    StructField("emp_sal",IntegerType,true),
    StructField("emp_location",StringType,true)
  ))

  val csv_pipe_delimiter_file_path = "E:\\pract_Dataset\\user_detail_pipe_delimiter.csv"

  val users_df_2 = spark.read
    .option("sep","|")
    .option("header",true)
    .schema(user_schema)
    .csv(csv_pipe_delimiter_file_path)

  users_df_2.show()
  users_df_2.printSchema()

  spark.stop()
  println("Application Completed")









}
