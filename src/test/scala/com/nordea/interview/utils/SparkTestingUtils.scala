package com.nordea.interview.utils

import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, ColumnComparer}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{Assertions, BeforeAndAfterEach, Matchers, WordSpec}

trait SparkTestingUtils extends WordSpec
  with DataFrameComparer
  with ColumnComparer
  with BeforeAndAfterEach
  with Matchers
  with Assertions {

  private val master = "local[2]"
  private val appName = "testApp"
  implicit var spark: SparkSession = _
  var sc: SparkContext = _

  object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  override def beforeEach(): Unit = {
    println("Starting new spark session before running test")
    spark = new SparkSession
      .Builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    sc = spark.sparkContext
  }

  override def afterEach(): Unit = {
    println("Stopping spark session after running test")
    spark.stop()
  }
}
