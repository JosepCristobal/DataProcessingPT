# DataProcessingPT
Entrega de la práctica de Data Processing.
Trabajo realizado con Kafka, Spark y Scala.

## Requisitos de la práctica y desarrollo.

1. Crear el esquema de cada uno dels datasets, según descripción del detalle.
	
	Se han creado los esquemas solicitados y se ha generado una "case class" distribuidas cada una en un fichero distinto. Se encuentran en la carpeta src/main/scala/spyCelebram/model
	
2. Rellenar con info dummy según esquema.

	Se han generado ficheros en formato csv y txt con información para poder ser cargada en la ejecución de la aplicación. Se encuentran en la carpeta data. En los ficheros de mensajes faltaría añadir a cada registro, el id del IoT, por cambios de estrategia, no se generaron en un inicio en estos ficheros. La carga de las palabras prohibidas y la de los IoTs se hece en el propio programa a través de Secuencias.
	
3. Con el fin de procesar en tiempo real toda la información, tendréis que conseguir elaborar una única fuente de información ‘completa’ con la que trabajar, por lo previamente habréis tenido que preparar la info, quitar duplicados (si los hubiere), agrupar, ordenar, etc y todo aquello que creáis necesario para el correcto y posterior proceso.

	Se ha procedido a la carga de la información de Usuario, Iots y en Streaming, la de los mensajes. Se han generado 3 DataSets distintos y a través de joins se han unido los tres en uno sólo. La informacíon que se ha introducido ya ha sido previamente filtrada.
	
4. El fin último es hallar por hora (ventana temporal) las 10 palabras más usadas en los mensajes de tal ventana. Una vez realizado este proceso, en caso de que la palabra más repetida coincida con alguna de las palabras de la lista negra, el sistema (nuestra aplicación) deberá enviar una notificación.

 Hemos podido crear una ventana temporal y realmente ha funcionado, pero nos ha limitado en poder seguir con la práctica. Hemos seguido por otro camino, sinembargo  la ordenación de las palabras más usadas (conteo) descendentemente y filtrado por las palabras prohibidas, se ha podido realizar, pero hacerlo con las TOP 10, me ha resultado imposible. Se ha buscado por todos los rincones de la red y no he podido encontrar la solución.
 
### Condiciones 		
• El sistema funciona 24/7
	
	Se trabaja para capturar los datos en Streaming a traves de Kafka
	
• Algunos IOT pueden dejar de funcionar, o bien por batería o bien porque se
apaguen en remoto (estado apagado). Los IOT apagados no deberán
contabilizar para la ingesta de datos.

	En el dataset de IoTs tenemos un campo donde nos indica si está encendido o
	apagado y al filtrar los mensajes, solo recogemos los que han sido
	transmitidos a través de IoTs operativos.
	
• Los sistemas de notificación serán simulados

	Son simulados
	
• El sistema de ‘desencriptado’ (función) será simulado

	En el código se indica donde llamaríamos a la función de desencriptado.
	
• La lista negra existirá realmente, y deberéis hacer la comprobación de
pertenencia a dicha lista por parte de la palabra más repetida.

	Esta lista se ha generado a través de una secuencia en el própio código y el
	filtrado se realiza al final, después del conteo y agrupación de las palabras. 
	
• La palabra repetida no podrá ser una preposición ni conjunción ni artículo.

	Las palabras se han transformado a minúsculas para poder hacer un conteo más
	preciso y se ha filtrado que la palabra mínima aceptada tien 4 letras o más.
	
• Para simular el envío de datos por parte de los IOT’s, enchufaremos ficheros de
texto para que los procese Kafka.

	Se han generado ficheros csv y txt con registros, para la carga en Kafka y han funcionado correctamente.
	
	
## Requisitos puesta en marcha de Kafka con carga de datos

1. Descarga de los ficheros necesarios y descompresión en un directorio de nuestro ordenador, en nuestro caso, hemos generado la carpeta kafka_2.12-2.3.0 en la raiz de nuestro usuario.
 
2. Arrancar ZooKeeper
	
		bin/zookeeper-server-start.sh config/zookeeper.properties
		
3. Arrancar Kafka

		bin/kafka-server-start.sh config/server.properties
		
4. Crear un Topic

		bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic spyCelebram01
		
5. Listar Topics existentes

		bin/kafka-topics.sh --list --bootstrap-server localhost:9092
		
6. Enviar Ficheros a Kafka como producer

		bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spyCelebram01 < messages.txt
 		
7. Enviar mensajes sueltos a través de la ventana de comandos a Kafka como producer

		bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spyCelebram01		

## Ubicación de proyecto final

Con el fin de hacer pruebas y maquetas, se ha generado un package llamado BancoDePruebas.

El proyecto final, está situado en el pakage spyCelebram.

## Muestra de resultados obtenidos
		
![Captura pantalla de resultados](https://github.com/JosepCristobal/DataProcessingPT/blob/master/varis/Captura%20de%20pantalla%202019-10-27%20a%20las%2022.07.22.png)
		
		



