package BancoDePruebas

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import spyCelebram.model.Messages

import scala.tools.nsc.Global
import scala.tools.nsc.typechecker.StructuredTypeStrings

object Streaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Contar Mensajes to DataSet")
      .master("local[*]")
      .getOrCreate()

    val esquema = new StructType()
      .add("id","Int")
      .add("message","String")
      .add("user_id","Int")


    import spark.implicits._
    //Ingesta del fichero ->RDD
    val inputDF = spark.readStream
      .format("csv")
      .schema(esquema)
      .load("data")


    //val InputDSFiltrado = inputDS.select($"message").filter($"user_id"> 4).rdd
    val InputDSFiltrado = inputDF.select($"message").filter($"user_id"> 4)

    val palabras = InputDSFiltrado.map(frase => frase.toString()).flatMap(palabras2 => palabras2.split("\\W+"))
      .map(palabra => palabra.toLowerCase).filter(tipoPal => tipoPal.length > 3)

   // val contadorPalabras = palabras.map(palabraTL => (palabraTL,1)).reduceByKey((a,b) => a + b)

    //val contadorPalabrasOrdenado = contadorPalabras.map(par => par.swap).sortByKey(false).take(20)

    val resultadoGuarda = InputDSFiltrado.writeStream
        .format("parquet")
        .option("checkpointLocation","checkpoint")
        .start("output")
        .awaitTermination()
   // contadorPalabrasOrdenado.foreach(registro => println(registro))
    // inputDS.show(20)
  }

}
