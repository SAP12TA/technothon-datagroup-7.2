package org.example
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object demo {


    def main(args:Array[String]): Unit = {

      val spark: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("SparkByExamples.com")
        .getOrCreate()
      //val sc = spark.sparkContext

      //read json file into dataframe
//      val df = spark.read.json("src/main/resources/zipcodes.json")
//      df.printSchema()
//      df.show(false)

//      //read multiline json file
//      val multiline_df = spark.read.option("header",true).csv("D:\\data_1.csv")
//      //multiline_df.printSchema()
//      multiline_df.show(50)


      //read multiple files
//      val df2 = spark.read.json(
//        "src/main/resources/zipcodes_streaming/zipcode1.json",
//        "src/main/resources/zipcodes_streaming/zipcode2.json")
//      df2.show(false)

      //read all files from a folder
//      val df3 = spark.read.json("src/main/resources/zipcodes_streaming/*")
//      df3.show(false)
//
//      //Define custom schema
//      val schema = new StructType()
//        .add("City", StringType, true)
//        .add("Country", StringType, true)
//        .add("Decommisioned", BooleanType, true)
//        .add("EstimatedPopulation", LongType, true)
//        .add("Lat", DoubleType, true)
//        .add("Location", StringType, true)
//        .add("LocationText", StringType, true)
//        .add("LocationType", StringType, true)
//        .add("Long", DoubleType, true)
//        .add("Notes", StringType, true)
//        .add("RecordNumber", LongType, true)
//        .add("State", StringType, true)
//        .add("TaxReturnsFiled", LongType, true)
//        .add("TotalWages", LongType, true)
//        .add("WorldRegion", StringType, true)
//        .add("Xaxis", DoubleType, true)
//        .add("Yaxis", DoubleType, true)
//        .add("Zaxis", DoubleType, true)
//        .add("Zipcode", StringType, true)
//        .add("ZipCodeType", StringType, true)
//
//      val df_with_schema = spark.read.schema(schema).json("src/main/resources/zipcodes.json")
//      df_with_schema.printSchema()
//      df_with_schema.show(false)
//
//      spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS (path 'src/main/resources/zipcodes.json')")
//      spark.sqlContext.sql("SELECT * FROM zipcode").show()

      //Write json file

//      multiline_df.write
//        .json("D:/zipcodes1.json")

     val df = spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/demo")
        .option("dbtable", "emp")
        .option("user", "postgres")
        .option("password", "Saptaparna@123")
        .option("driver", "org.postgresql.Driver")
        .load()
      df.show()


      val props = new Properties()
      props.setProperty("user", "postgres")
      props.setProperty("password", "Saptaparna@123")
      val url = "jdbc:postgresql://localhost:5432/demo"
      val df1=spark.read.option("header",true).csv("C:\\Users\\saptaparna_ghosh\\Downloads\\archive (3)\\acc_16.csv")
      df1.write
        .mode(SaveMode.Overwrite)
        .jdbc(url,"emp2",props)




    }


}
