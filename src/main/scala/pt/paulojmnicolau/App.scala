package pt.paulojmnicolau

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
  dataFrames(0).show()

  //Cria DataFrame Atividade 2
  dataFrames = dataFrames :+ atividade2(server)
  dataFrames(1).show()

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
  return df_2
 }

}
