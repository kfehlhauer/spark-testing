package com.kurtfehlhauer.example

import org.apache.spark.sql.SparkSession

object WordCountTestDriver extends App {
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("WordCount")
    .getOrCreate()

  val lines = Array("The quick brown fox jumps over the lazy dog",
    "The check is in the mail",
    "I only changed one line of code",
    "To be or not to be",
    "The dog ate my homework",
    "Premature optimization is the root of all evil")

  val rdd = spark.sparkContext.parallelize(lines, 5)

  val wordCountsRDD = Utilities.wordCount(rdd, spark)
  wordCountsRDD.foreach(println)

  spark.stop
}
