package pt.paulojmnicolau

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author Paulo Nicolau (paulojmnicolau)
 */
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


  def saveDataToParquet(df: DataFrame, fileName: String, compressao: String) = {
    df.coalesce(1)
      .write
      .option("header", "true")
      .option("encoder", "UTF-8")
      .option("compression", compressao)
      .mode(SaveMode.Overwrite)
      .parquet("files/" + fileName)
  }
}
