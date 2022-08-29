package pt.paulojmnicolau

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, TypedColumn}

class DataFilter (val server: SparkSession) {

  def filterUserReviewsColumn(df :DataFrame): DataFrame = {
    val colunas = MapeamentoColunas().getColunasUserReviews()
    df.select(
      col(colunas(0)).cast(StringType),
      col(colunas(1)).cast(DoubleType))
    .na.fill(0, Seq(colunas(1)))
      .groupBy(colunas(0))
      .avg(colunas(1))
  }
}