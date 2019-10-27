package BancoDePruebas

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import spyCelebram.model.Messages

object ContarPalabrasDataSet {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Contar Mensajes to DataSet")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //Ingesta del fichero
    val inputDS = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/Messages01.csv")
      .as[Messages]

    val InputDSFiltrado = inputDS.select($"message").filter($"user_id" > 0).rdd

    val palabras = InputDSFiltrado.map(frase => frase.toString()).flatMap(palabras2 => palabras2.split("\\W+"))
      .map(palabra => palabra.toLowerCase).filter(tipoPal => tipoPal.length > 3)

    val contadorPalabras = palabras.map(palabraTL => (palabraTL,1)).reduceByKey((a,b) => a + b)

    val contadorPalabrasOrdenado = contadorPalabras.map(par => par.swap).sortByKey(false).take(20)

    contadorPalabrasOrdenado.foreach(registro => println(registro))
   // inputDS.show(20)
  }
}
