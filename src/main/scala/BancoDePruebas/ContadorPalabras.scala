package BancoDePruebas

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ContadorPalabras {
  def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
    //Levantamos sesión en Spark
    val spark = SparkSession
      .builder()
      .appName("Contar Mensajes")
      .master("local[*]")
      .getOrCreate()

    //Ingesta del fichero ->DataSet
    val input = spark.sparkContext.textFile("data/Messages01.csv")
    val palabras = input.flatMap(frase => frase.split(","))
      .flatMap(frase2 => frase2.split("\\W+"))
      .map(palabra => palabra.toLowerCase).filter(tipoPal => tipoPal.length > 3)
    val contadorPalabras = palabras.map(palabra => (palabra,1)).reduceByKey((a,b) => a + b)

    //val contadorPalab = palabras.countByValue()
    val contadorPalabrasOrdenado = contadorPalabras.map(par => par.swap).sortByKey(false).take(20)
    //val contadorPalabrasOrdenado = contadorPalab.map(par => (par._2, par._1))

    contadorPalabrasOrdenado.foreach(registro => println(registro))

    spark.sparkContext.stop()


  }

}
