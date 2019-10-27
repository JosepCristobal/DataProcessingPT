package spyCelebram
import org.apache.log4j._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession,Row}
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.StructType
import spyCelebram.model.{Iot, Messages, MessagesProc, Users, BlackList}
import org.apache.spark.sql.functions._
import java.sql.Timestamp


object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Contar Mensajes Kafka")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","spyCelebram01")
      .option("includeTimestamp",true)
      .load()
      .selectExpr("CAST(value AS STRING) AS csv","CAST(timestamp AS STRING) AS timestamp","topic as topic" )
      .as[(String,String,String)]
    //Recogemos los datos transmitidos, junto a el timestamp y el topic.
    //El topic estuve a punto de utilizarlo para distiguir que IoT estaba transmitiendo, creando un topic para cada uno.
    //Al final decidí que lo mejor era identificarlos con un id.

    //Procesamos para ordenar los datos recogiso y dejarlos en un DataFrame
    val interval = data
      .selectExpr("split(csv,',')[0]"
        ,"split(csv,',')[1]"
        ,"split(csv,',')[2]","split(csv,',')[3]","timestamp","topic"
      ).toDF("id","message","user_id","id_Iot","time_Stamp","topic")

    //Transformamos el DataFrame interval a DataSet
    val intervalCast = interval.selectExpr("cast(id as int) id"
      ,"message"
      ,"cast(user_id as int) user_id"
      ,"cast(id_Iot as int) id_Iot"
      ,"cast(time_Stamp as Timestamp) time_Stamp"
      ,"topic"
    )
    val encoder = org.apache.spark.sql.Encoders.product[MessagesProc]

    val intervalDS = intervalCast.as(encoder)


    //Creamos un DataSet a través de una secuencia que pertenece a los IoTs.
    //Se decide hacerlo de esta forma por la poca cantidad de registros y buscar nuevas formas de hacerlo.
    val IotS = Seq(
      Iot(1,true,"North"),
      Iot(2,false,"South"),
      Iot(3,true,"Est"),
      Iot(4,true,"West"),
      Iot(5,true,"North-West")
    ).toDS

    //Creamos un DataSet a través de una secuencia que pertenece a los Mensajes no autorizados BlackList.
    //Se decide hacerlo de esta forma por la poca cantidad de registros.
    val listaNegra = Seq(
      BlackList("libero"),
      BlackList("volutpat"),
      BlackList("terrismo"),
      BlackList("pelotas")
    ).toDS()



    //Hacemos una búsqueda para determinar que IoTs están encendidos
    val IoT_ON = IotS.where($"Encendido" === true).select($"id_Iot").map(row=>row.getAs("id_Iot").toString).collect.mkString(",")
    println(s"Los IoTs activos son: ${IoT_ON}")

    //Transformamos un string en un array
    var arrIot = IoT_ON.split(",")

    //Este sería el filtro para recuperar los mensajes generados por los IoTs que están encendidos
    val resultadoIot = interval.filter(col("id_Iot").isin(arrIot:_*))

    //Esta sería una alternativa al filtro anterior para recuperar los mensajes generados por los IoTs que están encendidos
    // interval.createOrReplaceTempView("resProvisional")
    // val text = "SELECT * FROM resProvisional where id_Iot in (" + IoT_ON + ")"
    // val resultadoIot3 = spark.sql(text)

    //Ingesta del fichero de Usuarios para poder cruzar los DataSets Messages, IoT, Users y generar una única fuente de datos
    //Tal y como se pide en la práctica.

    //Recuperamos Usuarios a través de la lectura del .csv de Users
    val inputDSUsers = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/Users.csv")
      .as[Users]

    //La mostramos en la consola, para verificar que se ha cargado correctamente
    inputDSUsers.show()

    //Procedemos a hacer un join de los DataSets de Mensajes y Zona IoTs
    val messagesJoinIoTs = resultadoIot.join(IotS,resultadoIot("id_Iot") === IotS("id_Iot"), "inner")
    //Procedemos a hacer un join de los DataSets de Mensajes y Usuarios
    val messagesJoinUsers = messagesJoinIoTs.join(inputDSUsers,messagesJoinIoTs("user_id") === inputDSUsers("user_id"), "inner" )
    //Filtramos los campos del resultado del Join
    val messagesJoinSelect = messagesJoinUsers.select($"id",$"message",$"Zona",$"name",$"lastName",$"email",$"gender",$"age",$"time_Stamp",$"topic")

    //Ahora vamos a aislar los mensajes para poder hacer los cálculos con las palabras

    var InputDSFiltrado = messagesJoinSelect.select($"message",$"time_Stamp")

    val InputDSFiltrado_0 = messagesJoinSelect.select($"message")

    //En este punto deberíamos llamar a una función que pasándole el mensaje "message", que teóricamente viene encriptado, nos lo
    //devolviera desencriptado y legible.
    //-----------------------------------LLamada------------------------

    // A partir de este punto vienen las dudas y los problemas que hay que resolver
    //Si utilizo el DataFrame resultado de aplicar el intervalo de tiempo, no soy capaz de hacer un conteo de las palabras.
    //Todas las acciones sobre el dataset resultante, que no han sido pocas, me salta un error y no me deja seguir adelante.
    val conteoPalabrasCadaXseg = InputDSFiltrado
      .groupBy(window($"time_Stamp","20 seconds","15 seconds"),$"message").count().orderBy($"window")

    val misPalabras = conteoPalabrasCadaXseg.select($"message", $"window")

    //En este punto separamos las palabras de cada mensaje y desestimamos las que su length en menor de 3
    //aplicamos este criterio para omitir las preposiciones las conjunciones y los articulos.
    //En el Split aplicamos la expresion regular \\W+ para filtrar por letras, omitiendo otro tipo de elementos.
    //Para hacer un buen conteo y una posterior búsqueda en la lista negra, transformaremos todas las letras en minúsculas.

    val palabras = misPalabras.map(frase => (frase.toString())).flatMap(palabras2 => palabras2.split("\\W+"))
      .map(palabra => palabra.toLowerCase).filter(tipoPal => tipoPal.length > 3).toDF("word")

    val palabras_0 = InputDSFiltrado_0.map(frase => (frase.toString())).flatMap(palabras2 => palabras2.split("\\W+"))
      .map(palabra => palabra.toLowerCase).filter(tipoPal => tipoPal.length > 3).toDF("word_0")

    //Creamos una vista temporal y aplicamos una query SQL directamente para hacer el recuento de palabras.
    //No nos deja aplicar el TOP10, nos dice que es incompatible con Streaming.
    //Esta Query no la hemos podido aplicar al resultado del DataFrame resultante de intervalo de tiempo.
    //Nos da el error "Multiple streaming aggregations are not supported with streaming DataFrames/Datasets;;"
    //Removiendo y buscando en todos los rincones de internet, no he sabido dar con la solución.

    //Si prosigo con el DataFrame sin pasar por el intervalo de tiempo, me funciona correctamente, haciendo un recuento de palabras
    //y mostrandolo de forma correcta y ordenada descendentemente y respetando el intervalo de tiempo del anterior.
    palabras_0.createOrReplaceTempView("resProvisional")
    val text = "SELECT word_0, count(*) as contador FROM resProvisional GROUP BY word_0 ORDER BY contador DESC"

    val resultadoIot3 = spark.sql(text).toDF()



    //Hacemos una búsqueda para recoger las palabras prohibidas
    val palabrasProhibidas = listaNegra.select($"PalabraPr").map(row=>row.getAs("PalabraPr").toString).collect.mkString(",")
    println(s"Las palabras prohibidas son: ${palabrasProhibidas}")


    //Transformamos un string en un array
   // var arrPalPro = palabrasProhibidas.split(",").map(palfin => "'"+ palfin + "'")
    var arrPalPro = palabrasProhibidas.split(",")


    //Este sería el filtro para recuperar los mensajes que contienen las palabras prohibidas
    val resultadoPalProh = resultadoIot3.filter(col("word_0").isin(arrPalPro:_*)).toDF()

  //Hata este punto funciona el filtro de las palabras prohibidas, pero no hemos conseguido los top10. El filtro se hace sobre el DataFrame

    val query = resultadoPalProh.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation","checkpoint")
      .start()
      .awaitTermination()


  }

}
