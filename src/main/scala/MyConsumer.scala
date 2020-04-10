import java.util.{Collections, Properties}
import java.util.regex.Pattern
import java.nio.file.{Paths, Files}
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import com.google.gson.Gson

import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter

object MyConsumer extends App {

   println("I consume")
  val gson = new Gson()

  val existViolation = Files.exists(Paths.get("violationDrone.csv"))
  val existMessage = Files.exists(Paths.get("messageDrone.csv"))
  val existImage = Files.exists(Paths.get("image.csv"))

  var outputViolationFile = new BufferedWriter(new FileWriter("violationDrone.csv", true))
  var outputMsgFile = new BufferedWriter(new FileWriter("messageDrone.csv", true))
  var outputImageFile = new BufferedWriter(new FileWriter("image.csv", true))
  val csvMsgWriter = new CSVWriter(outputMsgFile, ',','\u0000','\u0000',"\n")
  val csvImageWriter = new CSVWriter(outputImageFile, ',','\u0000','\u0000',"\n")
  val csvViolationWriter = new CSVWriter(outputImageFile, ',','\u0000','\u0000',"\n")

  if(existViolation == false){
    val csvFields = Array("id_Drone","Issue Date","Violation Time", "latitude", "longitude","id_image", "Violation Code", "Plate ID")
    csvViolationWriter.writeNext(csvFields.mkString(","))
  }
  if(existImage == false){
    val csvFields = Array("id_image","image")
    csvImageWriter.writeNext(csvFields.mkString(","))
  }
  if(existMessage == false){
    val csvFields = Array("id_Drone","Date","Time", "latitude", "longitude")
    csvMsgWriter.writeNext(csvFields.mkString(","))
  }
  


  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","127.0.0.1:9092")
  props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer") 
  props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("Drone_Data")
  try {
    consumer.subscribe(topics.asJava)
    var i = 0
    while (i < 5) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        val dronemsg = gson.fromJson(record.value().toString(), classOf[DroneMessage])
      
        if(dronemsg.violationCode == null){
        //This is a regular message
            val recordArrayString = dronemsg.toArray().mkString(",")
            csvMsgWriter.writeNext(recordArrayString)

        }else{

          if(dronemsg.violationCode == -1){
            //The drone sends a Image
            var imageRecord = Array(dronemsg.id_image, dronemsg.image)
            csvImageWriter.writeNext(imageRecord.mkString(","))
          }else  {
            if(dronemsg.violationCode == -2){
            //The drone sends an alert
            println("ALERTE !! Men needed lat:" + dronemsg.latitude + " long: "+dronemsg.longitude)
            }


            // We write the message in the csv
            val recordArrayString = dronemsg.toArray().mkString(",")
            csvViolationWriter.writeNext(recordArrayString)
        }
        }
        i = i + 1
      }
    }
  }catch{
    case e:Exception => {
      outputMsgFile.close()
      outputViolationFile.close()
      outputImageFile.close()
      e.printStackTrace()}
  }finally {
    outputMsgFile.close()
    outputViolationFile.close()
    outputImageFile.close()
    consumer.close()
  }
}
