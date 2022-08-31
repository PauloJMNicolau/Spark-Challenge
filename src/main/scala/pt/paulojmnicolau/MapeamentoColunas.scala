package pt.paulojmnicolau

import org.apache.spark.sql.types.{ArrayType, DateType, DoubleType, LongType, StringType, StructField, StructType}
/**
 * @author Paulo Nicolau (paulojmnicolau)
 */
//Mapeamento das colunas dos ficheiros para cada DataFrame
case class MapeamentoColunas(){

  def getColunasUserReviews(): Array[String] =
    Array("App","Sentiment_Polarity")

  def getColunasGooglePlayStoreDf2(): Array[String] =
    Array("App",
      "Category",
      "Rating",
      "Reviews",
      "Size",
      "Installs",
      "Type",
      "Price",
      "Content Rating",
      "Genres",
      "Last Updated",
      "Current Ver",
      "Android Ver",
    )

}
