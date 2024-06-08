package com.*.interview

import com.*.interview.utils.SparkTestingUtils
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.*.interview.InterviewProblems.{countWordsUdf, preprocessAlertsDataOriginal, preprocessTaggedTransactionsData, preprocessTaggedTransactionsDataImproved, countWordsTransform, preprocessAlertsDataImproved}
import org.apache.spark.sql.functions.col

class InterviewProblemsTest extends SparkTestingUtils {

  "preprocessTaggedTransactionsData" should {
    "aggregate tagged transaction features" in {

      val originalDataFrameSchema = List(
        ("customer_id", StringType, true),
        ("txn_id", StringType, false),
        ("txn_amount_base", DoubleType, true),
        ("type_1", StringType, true),
        ("type_2", StringType, true),
        ("special_type", StringType, true),
        ("account_type_x", StringType, true)
      )

      val originalDataFrameData = List(
        ("customer_1", "A001", 4.0, "true", null, null, null),
        ("customer_1", "B002", 5.0, null, "true", null, null),
        ("customer_1", "C003", 5.0, null, "true", null, null),
        ("customer_1", "D003", 10.0, "true", "true", null, null),
        ("customer_2", "E004", 4.0, "true", null, null, null),
        ("customer_3", "F005", 5.0, "true", null, null, null),
        ("customer_4", "G006", 6.0, null, "true", null, null),
        ("customer_5", "H001", 4.0, "true", null, "true", null),
        ("customer_5", "I002", 5.0, null, "true", "true", null),
        ("customer_5", "J003", 5.0, null, "true", "true", null),
        ("customer_5", "K003", 10.0, "true", "true", "true", "true")
      )

      val expectedDataFrameSchema = List(
        ("customer_id", StringType, true),
        ("tagged_txns_count", LongType, false),
        ("tagged_txns_mean", DoubleType, false),
        ("tagged_txns_median", DoubleType, false),
        ("tagged_txns_sum", DoubleType, false),
        ("type_1_count", LongType, false),
        ("type_1_mean", DoubleType, false),
        ("type_1_median", DoubleType, false),
        ("type_1_sum", DoubleType, false),
        ("type_2_count", LongType, false),
        ("type_2_mean", DoubleType, false),
        ("type_2_median", DoubleType, false),
        ("type_2_sum", DoubleType, false),
        ("special_type_corrected_count", LongType, false),
        ("special_type_corrected_mean", DoubleType, false),
        ("special_type_corrected_median", DoubleType, false),
        ("special_type_corrected_sum", DoubleType, false)
      )

      val expectedDataFrameData = List(
        ("customer_1", 4.toLong, 6.0, 5.0, 24.0, 2.toLong, 7.0, 4.0, 14.0, 3.toLong, 6.0 + 2.0 / 3.0, 5.0, 20.0, 0L, 0.0, 0.0, 0.0),
        ("customer_2", 1.toLong, 4.0, 4.0, 4.0, 1.toLong, 4.0, 4.0, 4.0, 0.toLong, 0.0, 0.0, 0.0, 0L, 0.0, 0.0, 0.0),
        ("customer_3", 1.toLong, 5.0, 5.0, 5.0, 1.toLong, 5.0, 5.0, 5.0, 0.toLong, 0.0, 0.0, 0.0, 0L, 0.0, 0.0, 0.0),
        ("customer_4", 1.toLong, 6.0, 6.0, 6.0, 0.toLong, 0.0, 0.0, 0.0, 1.toLong, 6.0, 6.0, 6.0, 0L, 0.0, 0.0, 0.0),
        ("customer_5", 4.toLong, 6.0, 5.0, 24.0, 2.toLong, 7.0, 4.0, 14.0, 3.toLong, 6.0 + 2.0 / 3.0, 5.0, 20.0, 3L, 4.0 + 2.0 / 3.0, 5.0, 14.0)
      )



      val tagsToCollect = Seq("type_1", "type_2")
      val originalDataFrame = spark.createDF(originalDataFrameData, originalDataFrameSchema)
      val expectedDataFrame = spark.createDF(expectedDataFrameData, expectedDataFrameSchema)

      val selection = expectedDataFrameSchema.map(x => col(x._1))

      val taggedTransactionsDataFrame = preprocessTaggedTransactionsData(originalDataFrame, tagsToCollect).select(selection: _*)
      assertSmallDataFrameEquality(taggedTransactionsDataFrame, expectedDataFrame, orderedComparison = false)


      //TODO implement this
      val taggedTransactionsDataFrameImproved = preprocessTaggedTransactionsDataImproved(originalDataFrame, tagsToCollect).select(selection: _*)
      assertSmallDataFrameEquality(taggedTransactionsDataFrameImproved, expectedDataFrame)

    }
  }

  "countWordsTransform" should {
    "find percentage of name words that comes before | in a string" in {

      val originalDataFrameData = List(
        ("DK1", "jax", "jax.sons|jax.sons.anarchy", 100.0),
        ("DK1", "jax", "sons|anarchy", 0.0),
        ("DK1", "jax sons of anarchy teller", "jax_teller|anarchy", 40.0),
        ("DK2", "saul", "better|call", 0.0),
        ("DK2", "saul", "", 0.0),
        ("DK2", "", "better|call", 0.0),
        ("DK3", null, "better|call", 0.0),
        ("DK3", "", "", 0.0),
        ("DK4", "saul", null, 0.0),
        ("DK5", null, null, 0.0)
      )

      val originalDataFrameSchema = List(
        ("customer_id", StringType, true),
        ("name", StringType, true),
        ("string", StringType, true),
        ("percentage_of_name_words_in_string", DoubleType, false)
      )

      val expected = spark.createDF(originalDataFrameData, originalDataFrameSchema)

      val inputDF = expected.drop("percentage_of_name_words_in_string")

      val transformedDataFrameUDF = inputDF.withColumn("percentage_of_name_words_in_string", countWordsUdf(col("name"), col("string")))
      assertSmallDataFrameEquality(transformedDataFrameUDF, expected, orderedComparison = false, ignoreNullable = true)

      //TODO implement this
      val transformedDataFrameImproved = inputDF.transform(countWordsTransform("name", "string"))
      assertSmallDataFrameEquality(transformedDataFrameImproved, expected, orderedComparison = false, ignoreNullable = true)

    }
  }

  "preprocessAlertsData2" should {
    "count and rename alerts in category list, total alert count and null category alerts" in {

      val inputData = List(
        ("DK1", "1", "A1"),
        ("DK1", "2", null),
        ("DK1", "3", "A2"),
        ("DK2", "4", "A3"),
        ("DK2", "5", "A4"),
        ("DK3", "6", null)
      )

      val inputSchema = List(
        ("customer_id", StringType, false),
        ("alert_identifier", StringType, false),
        ("alert_classifier", StringType, true)
      )

      val resData = List(
        ("DK1", 3L, 1L, 1L, 1L, null),
        ("DK2", 2L, null, null, null, 1L),
        ("DK3", 1L, 1L, null, null, null)
      )

      val resSchema = List(
        ("customer_id", StringType, false),
        ("alerts_count", LongType, true),
        ("unknown_alerts_count", LongType, true),
        ("B1_alerts_count", LongType, true),
        ("B2_alerts_count", LongType, true),
        ("B3_alerts_count", LongType, true)
      )

      val alertsMap = Map(("A1" -> "B1"),
                          ("A2" -> "B2"),
                          ("A3" -> "B3"))


      val inputDF = spark.createDF(inputData, inputSchema)

      val expected = spark.createDF(resData, resSchema).cache()

      val selection = resSchema.map(x => col(x._1))

      val transformedDataFrameOriginal = inputDF.transform(preprocessAlertsDataOriginal(alertsMap)).select(selection: _*)
      assertSmallDataFrameEquality(transformedDataFrameOriginal, expected, orderedComparison = false, ignoreNullable = true)

      //TODO implement this
      val transformedDataFrameImproved = inputDF.transform(preprocessAlertsDataImproved(alertsMap)).select(selection: _*)
      assertSmallDataFrameEquality(transformedDataFrameImproved, expected, orderedComparison = false, ignoreNullable = true)

    }
  }

}
