package pt.paulojmnicolau

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Paulo Nicolau (paulojmnicolau)
 */
object App {

 def main(args: Array[String]) : Unit ={
  val server = criarServidor()                 //Cria Servidor Spark
  var dataFrames = new Array[DataFrame](0)     //Cria array de DataFrames para acesso pelas diferentes atividades

  //Criar DataFrame Atividade 1
  dataFrames = dataFrames :+ atividade1(server)
  dataFrames(0).show()

  //Cria DataFrame Atividade 2
  dataFrames = dataFrames :+ atividade2(server)
  dataFrames(1).show(500)

  //Cria DataFrame Atividade 3
  dataFrames = dataFrames :+ atividade3(server)
  dataFrames(2).show(500)

  //Cria DataFrame Atividade 4
  dataFrames = dataFrames :+ atividade4(server, dataFrames)
  dataFrames(3).show(500)

  //Cria DataFrame Atividade 5
  atividade5(server, dataFrames).show(500)
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
  val fileWriter = WriteFiles(server)
  fileWriter.saveDataToCsv(df_2, "best_apps", "§")
  df_2
 }

 //Executa Atividade 3
 def atividade3(server: SparkSession): DataFrame = {
  val df_3 = CreateDataFrames(server).createGooglePalyStoreDataFrame()
  df_3
 }

 //Executa Atividade 4
 def atividade4(server:SparkSession, dataFrames :Array[DataFrame] ): DataFrame ={
  val df = CreateDataFrames(server).createJoinDataframe(dataFrames(0), dataFrames(2))
  val fileWriter = WriteFiles(server)
  fileWriter.saveDataToParquet(df,"googleplaystore_cleaned","gzip")
  df
 }

 //Executa Atividade 5
 def atividade5(server:SparkSession, dataFrames: Array[DataFrame]) : DataFrame ={
  val df = CreateDataFrames(server).createEstatisticasDataFrame(dataFrames(3))
  val fileWriter = WriteFiles(server)
  fileWriter.saveDataToParquet(df,"googleplaystore_metrics","gzip")
  df
 }
}
