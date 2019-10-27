package BancoDePruebas

import java.sql.Timestamp

import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.StructType
import spyCelebram.model.Messages
import org.apache.spark.sql.functions._


object Kafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Contar Mensajes Kafka")
      .master("local[*]")
      .getOrCreate()

    val esquema = new StructType()
      .add("id","Int")
      .add("message","String")
      .add("user_id","Int")

    import spark.implicits._

    val data = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","spyCelebram01")
      .load()
      //.selectExpr("CAST(value AS STRING)","CAST(timestamp AS STRING)" )
     // .as[(String,String)]
    val interval= data.select(col("value").cast("string")).alias("csv").select("csv.*")

   /* val interval2 = interval
        .selectExpr("split(value,',')[0]"
        ,"split(value,',')[1]"
          ,"split(value,',')[2]",current_timestamp().toString()
        ).toDF("id","message","user_id","time_Stamp")*/

//.as[String]
    /*
    //proceso
    val palabras = data.as[(String, Timestamp)]
      .flatMap(par => par._1.split(",")
        .map(palabra => (palabra,par._2)))
      .toDF("palabra","timestamp")

    //ventana de tiempo
    val conteoPalabrasCada10seg = palabras
      .groupBy(window($"timestamp","10 seconds","5 seconds"),$"palabra")
      .count()
      .orderBy("window") */

   // data.show(20)
    //val InputDSFiltrado = data.select($"message").filter($"user_id" > 0)


    //Si queremos suscribirnos a más topicos lo haremos
    //.option("subsribeType","spyCelebram01,spyCelebram01")

    val query = interval.writeStream
      .outputMode("update")
      .format("console")
      .option("checkpointLocation","checkpoint")
      .start()
      .awaitTermination()

  }
}
