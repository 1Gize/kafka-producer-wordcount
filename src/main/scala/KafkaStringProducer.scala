import java.util.Properties
import org.apache.kafka.clients.producer._

import scala.io.StdIn.readLine
object KafkaStringProducer extends App {
  val props = new Properties()
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  val producer = new KafkaProducer[String,String](props)
  while(true){
    val inputString = readLine("Please enter your string")
    producer.send(new ProducerRecord[String,String]("WordCounter",inputString))
  }
}
