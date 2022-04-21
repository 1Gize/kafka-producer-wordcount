import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern

object KafkaWordCounterConsumer extends App{
  def wordCounter(s: String) = {
    val wordMap = s.split("\\W+")
      .foldLeft(Map.empty[String, Int]) {
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
      }
    wordMap.foreach(println)
  }
  val props = new Properties()
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  val consumer = new KafkaConsumer(props)
  import scala.jdk.CollectionConverters._
  consumer.subscribe(List("WordCounter").asJava)
  // var running = true
  while(true){
    val records = consumer.poll(Duration.ofMillis(1000)).asScala
    records.foreach(record => wordCounter(record.value()))
  }
  consumer.close()
}
