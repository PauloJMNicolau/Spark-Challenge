package pt.paulojmnicolau

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class WriteFiles(private val server : SparkSession) {
  def saveDataToCsv(df: DataFrame, fileName : String, delimiter:String)={
    val colunas = MapeamentoColunas().getColunasGooglePlayStoreDf2()
    df.coalesce(1)
      .orderBy(col(colunas(2)).desc)
      .write
        .option("header", "true")
        .option("delimiter" , delimiter)
        .option("encoder","UTF-8")
        .mode(SaveMode.Overwrite)
        .csv("files/" + fileName)
  }
}
