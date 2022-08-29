package pt.paulojmnicolau

//Mapeamento das colunas dos ficheiros para os DataFrames
case class MapeamentoColunas(){

  def getColunasUserReviews(): Array[String] =
    Array("App","Sentiment_Polarity")

  def getColunasGooglePlayStore: Array[String] =
    Array("App", "Sentiment_Polarity")


}
