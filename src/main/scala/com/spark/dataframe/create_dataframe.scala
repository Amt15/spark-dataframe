package com.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class User(user_id: Int, user_name: String, user_city: String)


object create_dataframe extends App {

  // How to create DataFrame

  val spark = SparkSession
    .builder()
    .appName("Create First Apache Spark DataFrame")
    .master("local[*]")
    .getOrCreate()

  // Don't want to see the info message for that we set logLevel "ERROR"
  spark.sparkContext.setLogLevel("ERROR")

  println("Approach 1")
  // creating List of tuple
  val users_list = List(
    (1, "Sumit", "jehanabad")
    ,(2, "Ajay", "Darbhanga")
    ,(3, "Chandan", "Samastipur")
    ,(4, "Abhishek", "Jharkhand")
    ,(5, "john","Londan")
    ,(6, "Sam", "Sydney")
  )
  val df_columns = Seq("user_id","user_name","user_city")
  val users_rdd = spark.sparkContext.parallelize(users_list)
  val users_df = spark.createDataFrame(users_rdd)
  users_df.show(6,truncate=false)
  println(users_df.getClass)

  val users_df_1 = users_df.toDF(df_columns:_*)
  users_df_1.show(6,truncate=false)
  println(users_df_1.getClass)

  println("Approach 2")

  val users_seq = Seq(Row(1,"john","Londan"),
    Row(2, "Martin", "New York"),
    Row(3, "Sam", "Sydney"),
    Row(4, "Ben", "Maxico City"),
    Row(5, "Jacob", "Florida")
  )

  val users_schema = StructType(Array(
    StructField("user_id",IntegerType,true),
    StructField("User_name",StringType,true),
    StructField("user_city",StringType,true))
  )

  val users_df_2 = spark.createDataFrame(spark.sparkContext.parallelize(users_seq), users_schema)
  users_df_2.show(5,truncate=false)
  println(users_df_2.getClass)

  println("Approach 3")

  val case_users_seq = Seq(
    User(1, "Amit", "Nalanda"),
    User(2, "Abhishek","Rohtas"),
    User(3, "Anup", "Jehanabad"),
    User(4, "Pankaj", "Patna"),
    User(5, "Aryan", "Ara")
  )

  val case_users_rdd = spark.sparkContext.parallelize(case_users_seq)
  val case_users_df = spark.createDataFrame(case_users_rdd)
  println(case_users_df.getClass)
  case_users_df.show(5,truncate=false)

  spark.stop()
  println("Application Completed")



  //======================





}
