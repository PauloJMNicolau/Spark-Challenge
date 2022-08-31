package pt.paulojmnicolau

import org.apache.spark.sql.{DataFrame, SparkSession}

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
  def createGooglePalyStoreDataFrame():DataFrame={
    val df = fileReader.getGooglePlayStore
    dataFilter.filterGooglePlayStoreColumnsTyped(df)
  }


}