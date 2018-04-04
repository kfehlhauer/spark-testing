package com.kurtfehlhauer.example

import org.apache.spark.sql.SparkSession

object ConsoleWordCount extends App {
  val spark = if (args.length > 0 && args(0) == "local") {
    SparkSession
      .builder
      .master("local[2]") // Gives me two threads to execute on
      .appName("WordCount")
      .getOrCreate
  }
  else {
    SparkSession
      .builder
      .appName("WordCount")
      .getOrCreate
  }

  val lines = Array("The quick brown fox jumps over the lazy dog",
    "The check is in the mail",
    "I only changed one line of code")

  val textRDD = spark.sparkContext.parallelize(lines, 5)

  val wc = textRDD.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  wc.foreach(println)

  spark.stop
}
