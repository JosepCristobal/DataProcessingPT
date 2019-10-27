package spyCelebram.model

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class MessagesProc(id:Int,message:String,user_id:Int,id_Iot:Int,time_Stamp:Timestamp,topic:String)
