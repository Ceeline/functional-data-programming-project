import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.google.gson.Gson


object MyProducer extends App {

  println("I produce")
  val droneMsg = Array(new DroneMessage(1, "07/20/2016", "0812A", 34.007623,-118.499757,null,"1243Y", 0, "BC-197-PO"),
                       new DroneMessage(3, "07/20/2016", "0913A", 34.002348,-118.499757,null,null, null, null),
                       new DroneMessage(2, "07/20/2016", "0815A", 34.007648,-118.499757,null,"1283Y", -2,null),
                       new DroneMessage(1, "07/20/2016", "1012A", 34.003248,-118.499757,null,"1293Y", 2, "BH-197-XO"),
                       new DroneMessage(1, "07/20/2016", "1012A", 34.003248,-118.499757,ImageStr.getImage(),"1243Y", -1, ""),
                       new DroneMessage(2, "07/20/2016", "1512A", 34.007632,-118.499757,null,null, null, null))

     
  val gson = new Gson()

  val props:Properties = new Properties()
  props.put("bootstrap.servers","127.0.0.1:9092")
  props.put("key.serializer",
         "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
         "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = "Drone_Data"
  try {
    for (i <- 0 to 5) {
      val droneSend = droneMsg(i)
      val record = new ProducerRecord[String, String](topic, i.toString, gson.toJson(droneSend))
      val metadata = producer.send(record)
      /*printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(), 
        metadata.get().partition(),
        metadata.get().offset())*/
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  } 
}
