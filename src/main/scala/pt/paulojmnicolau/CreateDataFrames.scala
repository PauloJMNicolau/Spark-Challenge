package pt.paulojmnicolau

import org.apache.spark.sql.{DataFrame, SparkSession}

case class CreateDataFrames(server : SparkSession){
  //Objetos constantes de leitor e filtros
  val fileReader: ReadFiles = ReadFiles (server)
  val dataFilter: DataFilter = new DataFilter(server)


  def createUserReviewsDataFrame(): DataFrame ={
    val df = fileReader.getUserReviews
    dataFilter.filterUserReviewsColumn(df)
  }


}