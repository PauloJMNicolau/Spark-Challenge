package pt.paulojmnicolau

import com.google.common.collect.ImmutableMap
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{coalesce, col, collect_list, isnull, lit, regexp_replace, to_date, when}
import org.apache.spark.sql.types.{ArrayType, DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, TypedColumn, functions}
/**
 * @author Paulo Nicolau (paulojmnicolau)
 */
case class DataFilter (private val server: SparkSession) {

  def filterUserReviewsColumns(df :DataFrame): DataFrame = {
    val colunas = MapeamentoColunas().getColunasUserReviews()
    df.select(                              //Selecionar Colunas Especificas
      col(colunas(0)).cast(StringType),     //Atribuir tipo String na coluna
      col(colunas(1)).cast(DoubleType))     //Atribuir tipo Double na coluna
    .na.fill(0, Seq(colunas(1)))      //Converte valores NULL e NaN da coluna em 0
      .groupBy(colunas(0))                  //Agrupa resultados por App
      .avg(colunas(1)).as(colunas(1))                      //Calcula média
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

  def filterGooglePlayStoreColumnsTyped(df: DataFrame): DataFrame = {
    df.select("*").na.replace("Rating",ImmutableMap.of("NaN", "0.0"))
      .withColumn("Size", regexp_replace(col("Size"), "\\D+",""))
      .withColumn("Price", regexp_replace(col("Price"), "\\D+",""))
      .orderBy(col("Reviews").desc)
      .groupBy(col("App").cast(StringType).as("App"))
      .agg(
        collect_list(col("Category")).cast(StringType).as("Categories"),
        coalesce(functions.max(col("Rating").cast (DoubleType)), lit (null)).as ("Rating"),
        coalesce(functions.max(col("Reviews").cast(LongType)), lit(0)).as("Reviews"),
        coalesce(functions.max(col("Size").cast(DoubleType)), lit(null)).as("Size"),
        coalesce(functions.max(col("Installs").cast(StringType)), lit(null)).as("Installs"),
        coalesce(functions.max(col("Type").cast(StringType)), lit(null)).as("Type"),
        coalesce(functions.max(col("Price").cast(DoubleType)).multiply(0.9), lit(null)).as("Price"),
        coalesce(functions.max(col("Content Rating").cast(StringType)), lit(null)).as("Content_Rating"),
        coalesce(collect_list(col("Genres")).cast(StringType), lit(null)).as("Genres"),
        to_date(coalesce(functions.max(col("Last Updated")), lit(null)), "MM DD,yyyy").as("Last_Updated"),
        coalesce(functions.max(col("Current Ver").cast(StringType)), lit(null)).as("Current_Version"),
        coalesce(functions.max(col("Android Ver").cast(StringType)), lit(null)).as("Minimum_Android_Version")
      ).dropDuplicates().orderBy(col("App").asc)
  }
}

