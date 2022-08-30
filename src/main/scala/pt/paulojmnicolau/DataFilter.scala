package pt.paulojmnicolau

import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, TypedColumn}

class DataFilter (val server: SparkSession) {

  def filterUserReviewsColumns(df :DataFrame): DataFrame = {
    val colunas = MapeamentoColunas().getColunasUserReviews()
    df.select(                              //Selecionar Colunas Especificas
      col(colunas(0)).cast(StringType),     //Atribuir tipo String na coluna
      col(colunas(1)).cast(DoubleType))     //Atribuir tipo Double na coluna
    .na.fill(0, Seq(colunas(1)))      //Converte valores NULL e NaN da coluna em 0
      .groupBy(colunas(0))                  //Agrupa resultados por App
      .avg(colunas(1))                      //Calcula média
  }

  def filterGooglePlayStoreColumns(df: DataFrame): DataFrame = {
    val colunas = MapeamentoColunas().getColunasGooglePlayStoreDf2()

    df.na
      //.fill(0, Seq(colunas(2)))
      .replace(
        colunas(2),ImmutableMap.of("NaN", "0.0"))           //Substitui NaN em 0.0
      .filter(                                                       //Filtra Resultados
        col(colunas(2))
          //.cast(DoubleType)
          .geq(4.0))            //por coluna de Rating >= 4.0
      .orderBy(col(colunas(2)).desc)                                //Ordena de forma descendente
  }
}