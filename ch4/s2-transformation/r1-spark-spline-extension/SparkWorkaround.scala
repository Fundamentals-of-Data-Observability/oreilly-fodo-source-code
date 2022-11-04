package org.apache.spark.sql // this to allow using the Dataset private object => and access ofRows

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object SparkWorkaround {
  // convert a plan into a DataFrame (corresponds to a read operation or write)
  def toDataFrame(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    val df = Dataset.ofRows(spark, plan) // Free tip 🥳
    df
  }
}