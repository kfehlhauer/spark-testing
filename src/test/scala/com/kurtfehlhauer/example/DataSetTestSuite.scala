package com.kurtfehlhauer.example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import com.holdenkarau.spark.testing.DatasetGenerator


class SampleDatasetGeneratorTest extends FunSuite with Checkers {
  test("Test generating Datasets[Custom Class]") {
    val numRows = 50
    implicit val generatorDrivenConfig = PropertyCheckConfig(minSize = 25, maxSize = numRows)
    val spark = SparkSession.builder().master("local[2]").appName("Test Datasets").getOrCreate()
    import spark.implicits._

    val carGen: Gen[Dataset[Person]] =
      DatasetGenerator.genDataset[Person](spark.sqlContext) {
        val generator: Gen[Person] = for {
          name <- Gen.alphaStr
          age <- Gen.choose(1, 100)
        } yield (Person(name, age))

        generator
      }

    val property =
      forAll(carGen) {
        ds =>
          println("*" * 100)
          ds.show(numRows, false)
          val count = ds.map(_.age).count()
          val combinedAge = ds.map(_.age).reduce(_ + _)
          val avgAge = combinedAge.toDouble / count
          println(s"Avg Age: $avgAge")
          avgAge < 100.0
      }

    check(property)
  }

  test("Test generating Datasets[Custom Class] 2") {
    val numRows = 50
    implicit val generatorDrivenConfig = PropertyCheckConfig(minSize = 25, maxSize = numRows)
    val spark = SparkSession.builder().master("local[2]").appName("Test Datasets 2").getOrCreate()
    import spark.implicits._

    val carGen: Gen[Dataset[Person]] =
      DatasetGenerator.genDataset[Person](spark.sqlContext) {
        val generator: Gen[Person] = for {
          name <- Gen.alphaStr
          age <- Gen.choose(1, 50)
        } yield (Person(name, age))

        generator
      }

    val property =
      forAll(carGen) {
        ds =>
          println("*" * 100)
          ds.show(numRows, false)
          val count = ds.map(_.age).count()
          val combinedAge = ds.map(_.age).reduce(_ + _)
          val avgAge = combinedAge.toDouble / count
          println(s"Avg Age: $avgAge")
          avgAge < 50.0
      }

    check(property)
  }
}

case class Person(name: String, age: Int)
