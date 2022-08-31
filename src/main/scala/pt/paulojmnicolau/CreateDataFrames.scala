package pt.paulojmnicolau

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Paulo Nicolau (paulojmnicolau)
 */
//Funções que tratam da obtenção dos dados de cada atividade
case class CreateDataFrames(server : SparkSession){
  //Objetos constantes de leitor e filtros
  val fileReader: ReadFiles = ReadFiles (server)
  val dataFilter: DataFilter = new DataFilter(server)

  //Cria DataFrame filtrado do ficheiro de  User Reviews
  def createUserReviewsDataFrame(): DataFrame ={
    val df = fileReader.getUserReviews
    dataFilter.filterUserReviewsColumns(df)
  }
  //Cria DataFrame filtrado (atividade 2) do ficheiro GooglePlayStore
  def createGooglePlayStoreBestAppDataFrame(): DataFrame = {
    val df = fileReader.getGooglePlayStore
    dataFilter.filterGooglePlayStoreColumns(df)
  }

  //Cria DataFrame filtrado (atividade 3) do ficheiro GooglePlayStore
  def createGooglePalyStoreDataFrame():DataFrame={
    val df = fileReader.getGooglePlayStore
    dataFilter.filterGooglePlayStoreColumnsTyped(df)
  }

  def createJoinDataframe(df_1: DataFrame, df_3: DataFrame) =
    df_1.as("df_1").join(df_3.as("df_3") , df_1("App") === df_3("App")).drop(col("df_3.App"))
}