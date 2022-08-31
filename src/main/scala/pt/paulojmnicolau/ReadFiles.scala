package pt.paulojmnicolau

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{FileNotFoundException, IOException}

/**
 * @author Paulo Nicolau (paulojmnicolau)
 */
//Funções de leitura dos ficheiros
case class ReadFiles(private val server : SparkSession) {

  //Ler ficheiro de Reviews
  def getUserReviews: DataFrame = try {
    server.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("files/googleplaystore_user_reviews.csv")
  } catch {
    case _: FileNotFoundException =>
      println("ERRO: Ficheiro de User Reviews não encontrado!")
      null
    case _: IOException =>
      println("ERRO: Falha a processar ficheiro de User Reviews!")
      null
  }

  //Ler ficheiro de App
  def getGooglePlayStore: DataFrame = try {
    server.read
      .option("delimiter", ",")
      .option("header", "true")
      .csv("files/googleplaystore.csv")
  } catch {
    case _: FileNotFoundException =>
      println("ERRO: Ficheiro de GooglePlay Store não encontrado!")
      null
    case _: IOException =>
      println("ERRO: Falha a processar ficheiro de GooglePlay Store!")
      null
  }
}
