package com.nordea.interview

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object InterviewProblems {

  //TO-DO Problem 1
  //Imagine that this needs to run on a billion transactions and a million customers, what is wrong with it?
  //Make a better one that pass the same unit test
  //Hint: Can you do it with a single groupBy and no joins?

  def preprocessTaggedTransactionsData(taggedTransactionsData: DataFrame, tagsToCollect: Seq[String]): DataFrame = {

    var grouped = taggedTransactionsData.groupBy(
      "customer_id").agg(
      count(col("txn_id")).as("tagged_txns_count"),
      mean(col("txn_amount_base")).as("tagged_txns_mean"),
      expr("percentile_approx(txn_amount_base, 0.5)").as("tagged_txns_median"),
      sum("txn_amount_base").alias("tagged_txns_sum")
    )

    for (columnName <- tagsToCollect) {
      grouped = grouped.join(
        taggedTransactionsData
          .filter(col(columnName) === "true")
          .groupBy("customer_id").agg(
            count(col("txn_id")).as(columnName + "_count"),
            mean(col("txn_amount_base")).as(columnName + "_mean"),
            expr("percentile_approx(txn_amount_base, 0.5)").as(columnName + "_median"),
            sum("txn_amount_base").alias(columnName + "_sum")
          ),
        Seq("customer_id"),
        "left")
    }

    grouped = grouped.join(
      taggedTransactionsData
        .filter(col("special_type") === "true" && col("account_type_x").isNull)
        .groupBy("customer_id").agg(
          count(col("txn_id")).as("special_type_corrected_count"),
          mean(col("txn_amount_base")).as("special_type_corrected_mean"),
          expr("percentile_approx(txn_amount_base, 0.5)").as("special_type_corrected_median"),
          sum("txn_amount_base").alias("special_type_corrected_sum")
        ),
      Seq("customer_id"),
      "left")

    grouped.na.fill(0)
  }

  //Improved Problem 1

  def preprocessTaggedTransactionsDataImproved(taggedTransactionsData: DataFrame, tagsToCollect: Seq[String]): DataFrame = {
      // Use the logic provided
      val aggregationExprs = Seq(
        count("txn_id").as("tagged_txns_count"),
        mean("txn_amount_base").as("tagged_txns_mean"),
        expr("percentile_approx(txn_amount_base, 0.5)").as("tagged_txns_median"),
        sum("txn_amount_base").as("tagged_txns_sum")
      )

      val taggedColumnsExprs = tagsToCollect.flatMap(tag => {
        // FlatMap the tagged transaction based on Type
        Seq(
          count(when(col(tag) === "true", 1)).as(s"${tag}_count"),
          mean(when(col(tag) === "true", col("txn_amount_base"))).as(s"${tag}_mean"),
          expr(s"percentile_approx(case when ${tag} = 'true' then txn_amount_base end, 0.5)").as(s"${tag}_median"),
          sum(when(col(tag) === "true", col("txn_amount_base"))).as(s"${tag}_sum")
        )
      })

      val specialTypeExprs = Seq(
        // Collect the special type tagged transactions
        count(when(col("special_type") === "true" && col("account_type_x").isNull, 1)).as("special_type_corrected_count"),
        mean(when(col("special_type") === "true" && col("account_type_x").isNull, col("txn_amount_base"))).as("special_type_corrected_mean"),
        expr(s"percentile_approx(case when special_type = 'true' AND account_type_x IS NULL then txn_amount_base end, 0.5)").as("special_type_corrected_median"),
        sum(when(col("special_type") === "true" && col("account_type_x").isNull, col("txn_amount_base"))).as("special_type_corrected_sum")
      )

      // Aggregate them all together
      val allAggregationExprs = aggregationExprs ++ taggedColumnsExprs ++ specialTypeExprs

      //Use one groupBy and no Joins to bring together
      val processedData = taggedTransactionsData
        .groupBy("customer_id")
        .agg(allAggregationExprs.head, allAggregationExprs.tail: _*)
        .na.fill(0)
        .orderBy("customer_id")

      processedData.show()
      processedData

    }

  //TO-DO Problem 2
  //Replace this udf with a column function using spark sql functions

  val countByCommaUDF: UserDefinedFunction = udf(countByCommaOld _)

  def countByCommaOld(s: String): Int = {
    if (isEmptyStr(s) || s.trim == "UNK") 0
    else s.split(",").distinct.map(_.trim).filterNot(isEmptyStr).count(_ != "UNK")
  }

  def isEmptyStr(s: String): Boolean = {
    if (s == null || s.trim.isEmpty) true else false
  }

  //Improved Problem 2

  val countByComma: Column => Column = (col: Column) => size(split(col, ",").cast("array<string>")) - 1

  //Problem 3
  //This udf calculate how large a percentage of a customers name parts occurs in a string before a | delimiter
  //Write a custom spark transform function that does the same
  //Hint: Can array functions be used? Check the signature in the Unit tests.

  val countWordsUdf: UserDefinedFunction = udf((name: String, string: String) => {
    if (name == null || name.isEmpty || string == null || string.isEmpty) {
      0.0
    } else {
      val nameWords = name.split(" ")
      val firstPartOfString = string.split("\\|")(0)
      val numNameWords = nameWords.count(word => firstPartOfString.contains(word))
      numNameWords.toDouble / nameWords.length * 100
    }
  })

  //Improved Problem 3

  val countWordsTransform: (String, String) => DataFrame => DataFrame = (name: String, string: String) => (inputDF: DataFrame) => {
    // Use UDF defined earlier
    val countWordsUDF = udf { (name: String, string: String) =>
      if (name == null || name.isEmpty || string == null || string.isEmpty) {
        0.0
      } else {
        val nameWords = name.split(" ")
        val firstPartOfString = string.split("\\|")(0)
        val numNameWords = nameWords.count(word => firstPartOfString.contains(word))
        numNameWords.toDouble / nameWords.length * 100
      }
    }
    inputDF.withColumn("percentage_of_name_words_in_string", countWordsUDF(col(name), col(string)))
    val transformedDF = inputDF.withColumn("percentage_of_name_words_in_string", countWordsUDF(col(name), col(string)))
    transformedDF.show()
    transformedDF
  }

  //Problem 4
  //What is wrong with this transformation? How does it scale?
  //Make a better version
  //Hint; Can you get rid of the for loop?

  def preprocessAlertsDataOriginal(alertsMap: Map[String, String])(alertsData: DataFrame): DataFrame = {
    var grouped = alertsData.groupBy(
      "customer_id").agg(
      count(col("alert_identifier")).as("alerts_count")
    )

    for (classifier <- alertsMap) {
      grouped = grouped.join(
        alertsData
          .filter(col("alert_classifier") === classifier._1)
          .groupBy("customer_id").agg(
          count(col("alert_identifier")).as(alertsMap(classifier._1) + "_alerts_count")
        ),
        Seq("customer_id"),
        "left")
    }

    val null_alerts = alertsData
      .filter(col("alert_classifier").isNull)
      .groupBy("customer_id").agg(count("alert_identifier").as("unknown_alerts_count"))

    grouped.join(null_alerts, Seq("customer_id"),
      "left")

  }

  //Improved Problem 4

  def preprocessAlertsDataImproved(alertsMap: Map[String, String])(alertsData: DataFrame): DataFrame = {
    // Use the aforementioned logic
    var grouped = alertsData.groupBy("customer_id").agg(
      count(col("alert_identifier")).as("alerts_count")
    )

    // Joining grouped DataFrame with filtered and aggregated alertsData based on each classifier in alertsMap
    alertsMap.foreach { case (classifier, newName) =>
      grouped = grouped.join(
        alertsData.filter(col("alert_classifier") === classifier)
          .groupBy("customer_id")
          .agg(count(col("alert_identifier")).as(s"${newName}_alerts_count")),
        Seq("customer_id"),
        "left"
      )
    }

    // Use aforementioned logic
    val nullAlerts = alertsData.filter(col("alert_classifier").isNull)
      .groupBy("customer_id")
      .agg(count("alert_identifier").as("unknown_alerts_count"))

    grouped = grouped.join(nullAlerts, Seq("customer_id"), "left")

    grouped.show()
    grouped
  }

}