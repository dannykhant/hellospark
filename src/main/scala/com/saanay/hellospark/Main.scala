package com.saanay.hellospark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.avg


object Main {
  def main(args: Array[String]): Unit = {

    print("Choose an example: ")
    val select = scala.io.StdIn.readInt()
    if (select == 1) {
      countWords()
    } else if (select == 2) {
      countStream()
    } else if (select == 3) {
      findSpent()
    } else if (select == 4) {
      queryData()
    } else {
      println("Unsupported..")
    }
  }

  // Basic Example
  private def countWords(): Unit = {
    val x = "data/input.txt"
    val conf = new SparkConf().setAppName("App1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val y = sc.textFile(x).cache()
    val cnt = y.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    cnt.saveAsTextFile("data/output-" + java.util.UUID.randomUUID.toString)
    sc.stop()
  }

  // Streaming Example
  private def countStream(): Unit = {
    val sp = SparkSession.builder().appName("App2").master("local[2]").getOrCreate()
    sp.sparkContext.setLogLevel("ERROR")
    import sp.implicits._
    val lines = sp.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wc = words.groupBy("value").count()
    val query = wc.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }

  // ML Example
  private def findSpent(): Unit = {
    /* Problem: To find Yearly Amount spent (dependent variable)
    based on info such as "Avg Session Length","Time on App","Time on Website",
    "Length of Membership" [independent variables]
    To check schema: data.printSchema()
    To see data: df2.show() */

    val sp = SparkSession.builder().appName("App3").master("local[2]").getOrCreate()
    import sp.implicits._
    val data = sp.read.option("header", "true").option("inferSchema", "true")
      .format("csv").load("data/EcommerceCustomers1.csv")
    val df = data.select(data("Yearly Amount Spent").as("label"), $"Avg Session Length", $"Time on App",
      $"Time on Website", $"Length of Membership")
    // For matrix format
    val vs = new VectorAssembler().setInputCols(Array("Avg Session Length", "Time on App", "Time on Website",
      "Length of Membership")).setOutputCol("features")
    // Transform features
    val df2 = vs.transform(df).select($"label", $"features")
    // LR model
    val lr = new LinearRegression()
    val lrModel = lr.fit(df2)
    val trainingSummary = lrModel.summary
    trainingSummary.residuals.show()
  }

  // SQL Example
  private def queryData(): Unit = {
    val sp = SparkSession.builder().appName("App4").master("local[2]").getOrCreate()
    import sp.implicits._
    // json
    val jsonData = sp.read.json("data/employees.json")
    jsonData.select($"name", $"salary" + 5).filter($"salary" > 4000).show()
    // csv
    val csvData = sp.read.format("csv").option("inferSchema", "true").option("header", "true")
      .load("data/BankData.csv")
    val over60 = csvData.filter($"age" > 60).count()
    val total = csvData.count.toDouble
    val over60Rate = (over60 / total) * 100.0
    println("over60Rate: " + over60Rate)
    csvData.select(avg($"balance")).show()
    // Temp View
    csvData.createOrReplaceTempView("bank_data")
    val under20 = sp.sql("select * from bank_data where age < 20 limit 10")
    under20.write.save("data/output-under20")
  }
}