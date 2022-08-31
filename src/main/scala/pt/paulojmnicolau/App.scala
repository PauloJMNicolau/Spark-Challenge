package pt.paulojmnicolau

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ${user.name}
 */
object App {

 def main(args: Array[String]) : Unit ={
  val server = criarServidor()                 //Cria Servidor Spark
  var dataFrames = new Array[DataFrame](0)     //Cria array de DataFrames para acesso pelas diferentes atividades

  //Criar DataFrame Atividade 1
  dataFrames = dataFrames :+ atividade1(server)
  //dataFrames(0).show()

  //Cria DataFrame Atividade 2
  dataFrames = dataFrames :+ atividade2(server)
  //dataFrames(1).show()

  dataFrames = dataFrames :+ atividade3(server)
//  dataFrames(2).show()

  dataFrames = dataFrames :+ atividade4(server, dataFrames)
  dataFrames(3).show()

//  atividade5(server, dataFrames).show()
  server.stop()                                 //Terminar Spark
 }

 //Cria e configura o servidor (Utilizado Spark 3.3.0 com Hadoop Scala 3.12.x)
 def criarServidor(): SparkSession=
  //Inicializar Spark
  SparkSession
   .builder()
   .appName("Spark Recruitment Challenge")
   .getOrCreate()

 //Executa Atividade 1
 def atividade1(server : SparkSession): DataFrame ={
  CreateDataFrames(server).createUserReviewsDataFrame()
 }

 //Executa Atividade 2
 def atividade2(server: SparkSession):DataFrame ={
  val df_2 = CreateDataFrames(server).createGooglePlayStoreBestAppDataFrame()
  val fileWriter = new WriteFiles(server)
  fileWriter.saveDataToCsv(df_2, "best_apps.csv", "§")
  df_2
 }

 //Executa Atividade 3
 def atividade3(server: SparkSession): DataFrame = {
  val df_3 = CreateDataFrames(server).createGooglePalyStoreDataFrame()
  df_3
 }

 def atividade4(server:SparkSession, dataFrames :Array[DataFrame] )={
  val df = dataFrames(0).as("df_1").join(dataFrames(2).as("df_3") , dataFrames(0)("App") === dataFrames(2)("App")).drop(col("df_3.App"))
  val fileWriter = new WriteFiles(server)
  fileWriter.saveDataToParquet(df,"googleplaystore_cleaned","gzip")
  df
 }

 /*def atividade5(server:SparkSession, dataFrames: Array[DataFrame]) : DataFrame ={
   dataFrames(3).select(col("Genres")).reduce((linha, linha1) =>{
    println(linha)
    println(linha1)
    linha
   })
 }*/
}
