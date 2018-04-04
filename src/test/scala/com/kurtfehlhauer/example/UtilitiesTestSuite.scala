package com.kurtfehlhauer.example

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class UtilitiesTestSuite extends FunSuite with BeforeAndAfterEach {
  var spark: SparkSession = _

  override def beforeEach() {
    spark = SparkSession
      .builder()
      .appName("WordCountUtilitesTestSuite Tests")
      .master("local[2]")
      .getOrCreate()
  }

  test("No Word Count") {
    val word = Seq[String]()
    val textRDD= spark.sparkContext.parallelize(word, 2)
    val wc = Utilities.wordCount(textRDD, spark).collect()

    assert(wc.length == 0)
  }

  test("Single Word Count") {
    val word = Seq("word")
    val textRDD= spark.sparkContext.parallelize(word, 2)
    val wc = Utilities.wordCount(textRDD, spark).collect()

    assert(wc.length == 1)
    assert(wc(0)._2 == 1)
  }

  test("Double Word Count") {
    val word = Seq("word word")
    val textRDD= spark.sparkContext.parallelize(word, 2)
    val wc = Utilities.wordCount(textRDD, spark).collect()

    assert(wc.length == 1)
    assert(wc(0)._2 == 2)
  }

  test("Multiple Word Count") {
    val word = Seq("word word dog dog dog cat cat cat cat cat mouse 3 3 3 4 4 4 4")
    val textRDD= spark.sparkContext.parallelize(word, 2)
    val wc = Utilities.wordCount(textRDD, spark).collect()

    println(wc.length)
    assert(wc.length == 6)
    val wcMap = wc.toMap
    assert(wcMap.get("word").get == 2)
    assert(wcMap.get("dog").get == 3)
    assert(wcMap.get("cat").get == 5)
    assert(wcMap.get("mouse").get == 1)
    assert(wcMap.get("3").get == 3)
    assert(wcMap.get("4").get == 4)
  }

  test("Random Number between 0 and ten") {
    1 to 1000 foreach {x =>
      val y = Utilities.numberBetweenZeroandTen(x)
      assert(y > -1 && y < 11)
    }
  }

  override def afterEach() {
    spark.stop()
  }

}

