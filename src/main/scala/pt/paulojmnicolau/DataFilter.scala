package pt.paulojmnicolau

import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
/**
 * @author Paulo Nicolau (paulojmnicolau)
 */
//Funções com todas as filtragens de dados para cada atividade
case class DataFilter (private val server: SparkSession) {

  //Filtragem para DataFrame 1
  def filterUserReviewsColumns(df :DataFrame): DataFrame = {
    val colunas = MapeamentoColunas().getColunasUserReviews
    df.select(                              //Selecionar Colunas Especificas
      col(colunas(0)).cast(StringType),     //Atribuir tipo String na coluna
      col(colunas(1)).cast(DoubleType))     //Atribuir tipo Double na coluna
    .na.fill(0, Seq(colunas(1)))      //Converte valores NULL e NaN da coluna em 0
      .groupBy(colunas(0))                  //Agrupa resultados por App
      .avg(colunas(1))                      //Calcula média
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity") // e atribui nome à coluna
  }

  //Filtragem para Dataframe 2
  def filterGooglePlayStoreColumns(df: DataFrame): DataFrame = {
    val colunas = MapeamentoColunas().getColunasGooglePlayStoreDf2
    df.na
      .replace(
        colunas(2),ImmutableMap.of("NaN", "0.0"))           //Substitui NaN em 0.0
      .filter(                                                       //Filtra Resultados
        col(colunas(2)).geq(4.0))                             //por coluna de Rating >= 4.0
      .orderBy(col(colunas(2)).desc)                                //Ordena de forma descendente
  }

  //Filtragem para DataFrame 3
  def filterGooglePlayStoreColumnsTyped(df: DataFrame): DataFrame = {
    df.select("*").na                                               //Seleciona todas as colunas
      //Remove caracteres indesejados na conversão para valores numéricos
      .replace("Rating",ImmutableMap.of("NaN", "0.0"))
      .withColumn("Size", regexp_replace(col("Size"), "\\D+",""))
      .withColumn("Price", regexp_replace(col("Price"), "\\D+",""))
      .orderBy(col("Reviews").desc)
      .groupBy(col("App").cast(StringType).as("App"))
      //Funções de Agregação de dados e conversão para o tipo de dados correto com atribuição de valores default
      .agg(
        functions.array_distinct(collect_list(col("Category").cast(StringType))).as("Categories"),
        coalesce(functions.max(col("Rating").cast (DoubleType)), lit (null)).as ("Rating"),
        coalesce(functions.max(col("Reviews").cast(LongType)), lit(0)).as("Reviews"),
        coalesce(functions.max(col("Size").cast(DoubleType)), lit(null)).as("Size"),
        coalesce(functions.max(col("Installs").cast(StringType)), lit(null)).as("Installs"),
        coalesce(functions.max(col("Type").cast(StringType)), lit(null)).as("Type"),
        coalesce(functions.max(col("Price").cast(DoubleType)).multiply(0.9), lit(null)).as("Price"),
        coalesce(functions.max(col("Content Rating").cast(StringType)), lit(null)).as("Content_Rating"),
        coalesce(functions.array_distinct(collect_list(col("Genres").cast(StringType))), lit(null)).as("Genres"),
        functions.max(coalesce(to_date(col("Last Updated"), "MM DD,yyyy"), lit(null))).as("Last_Updated"),
        coalesce(functions.max(col("Current Ver").cast(StringType)), lit(null)).as("Current_Version"),
        coalesce(functions.max(col("Android Ver").cast(StringType)), lit(null)).as("Minimum_Android_Version")
      ).dropDuplicates()                                                             //Eliminar linhas duplicadas
      .orderBy(col("App").asc)                                              //Realiza ordenação dos dados
  }

  //Filtragem para DataFrame 4
  def filterEstatisticas(df: DataFrame) : DataFrame ={
    df.withColumn("Genres", explode(col("Genres")))
      .groupBy("Genres")
      .agg(
        count(col("Genres")).as("Count"),
        avg(col("Rating")).as("Average_Rating"),
        avg(col("Average_Sentiment_Polarity")).as("Average_Sentiment_Polarity")
      )
  }

}


