import java.util.Properties
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStreamBuilder, KeyValueMapper}
import play.api.libs.json._
/**
  * Copyright Knoldus Software LLP, 2017. All rights reserved.
  */
object ChannelGroupProfileStream {
  def main(args: Array[String]): Unit = {
    val config = {
      val properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application")
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties
    }
    val stringSerde = Serdes.String()
    val integerSerde = Serdes.Integer()
    val builder = new KStreamBuilder()
    val originalStream = builder.stream("wanoperationaldb1")
    val mappedStream =
      originalStream.map[String, Integer] {
        new KeyValueMapper[String, String, KeyValue[String, Integer]] {
          override def apply(key: String, value: String): KeyValue[String, Integer] = {
		       val json: JsValue = Json.parse(value)
		        println(json("potentialmatrix") +"  "+json("dscpValues"))
            val wan_json= Json.parse(json("wanLinks").toString())
            println(wan_json)
            new KeyValue(key, new Integer(value.length))
          }
        }
      }
    mappedStream.to(stringSerde, integerSerde, "SinkTopic")
    val streams = new KafkaStreams(builder, config)
    streams.start()
  }
}
