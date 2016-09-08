package com.bakery.woople.sql

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
// Import Row.;

// Import Spark SQL data types;
// Import Row.;

// Import Spark SQL data types;


/**
  * Created by peng on 16/9/5.
  */
object SparkSqlGuide {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlGuide")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val people = sc.textFile("/user/ocdp/people.txt")

    val schemaString = "name age"

    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    results.map(t => "Name: " + t(0)).collect().foreach(println)



    val df = sqlContext.read.format("json").load("/user/ocdp/people.json")

    val parquetFile = sqlContext.read.parquet("/user/ocdp/namesAndAges.parquet")

    println("*" * 10)
    println(parquetFile.count())
    println("*" * 10)

    if(parquetFile.count() < 0){
      df.select("name", "age").write.format("parquet").save("/user/ocdp/namesAndAges.parquet")
    }


    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(new Path("/user/ocdp/adfdfad.parquet"))){
      println("$" * 100)
    }else{
      println("@" * 100)
    }






    val df2 = sqlContext.sql("SELECT * FROM parquet.`/user/ocdp/namesAndAges.parquet`")

    println("=" * 10)
    df2.show
    println("=" * 10)




  }

}
