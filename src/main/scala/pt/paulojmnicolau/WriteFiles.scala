package pt.paulojmnicolau

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.io.File

/**
 * @author Paulo Nicolau (paulojmnicolau)
 */
//Funções de escrita de ficheiros
case class WriteFiles(private val server : SparkSession) {

  //Guarda ficheiro CSV
  def saveDataToCsv(df: DataFrame, fileName : String, delimiter:String): Unit ={
    val colunas = MapeamentoColunas().getColunasGooglePlayStoreDf2
    df.coalesce(1)
      .orderBy(col(colunas(2)).desc)
      .write
        .option("header", "true")
        .option("delimiter" , delimiter)
        .option("encoder","UTF-8")
        .mode(SaveMode.Overwrite)
        .csv("files/" + fileName)
  }

  //Guarda ficheiro Parquet
  def saveDataToParquet(df: DataFrame, fileName: String, compressao: String): Unit = {
    df.coalesce(1)
      .write
      .option("header", "true")
      .option("encoder", "UTF-8")
      .option("compression", compressao)
      .mode(SaveMode.Overwrite)
      .parquet("files/" + fileName)
  }

}
