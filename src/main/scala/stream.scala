import java.util.Properties
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import play.api.libs.json._
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable,   ForeachAction}


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
    
    val jsonSerializer: Serializer[JsonNode] = new JsonSerializer()
    val jsonDeserializer: Deserializer[JsonNode] = new JsonDeserializer()
    val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)
    val wanOperationalDbTopic: String = "wan_op_db"
    val trafficShapingTopic: String ="traffic_shaping"
    val trafficClassTopic: String = "traffic_class"
    val trafficClassificationTopic: String = "traffic_Classification"
    val operationalSCPCTopic: String = "operational_scpc"
    val remoteTopic: String = "remote"

    val stringSerde = Serdes.String()
    val integerSerde = Serdes.Integer()
    val builder = new KStreamBuilder() 
   
    val trafficShapingStream: KStream[String, JsonNode] = builder.stream(stringSerde, jsonSerde, trafficShapingTopic);

    val trafficClassificationStream: KStream[String, JsonNode] = builder.stream(stringSerde, jsonSerde, trafficClassificationTopic)
    
    trafficShapingStream.foreach(new ForeachAction[String, JsonNode]() {
      override def apply(key: String, value: JsonNode): Unit = {
        println(key + " 1: " + value.get("nodename"))
      }
    });

    trafficClassificationStream.foreach(new ForeachAction[String, JsonNode]() {
      override def apply(key: String, value: JsonNode): Unit = {
        println(key + " 2: " + value.get("nodename"))
      }
    })
    
    //val trafficClassificationStream: KStream[String, JsonNode]  = trafficShapingStream.join(trafficClassificationStream -> trafficClassificationStream.path("nodename"));
    val  streams = new KafkaStreams(builder, config);
    streams.start();

    
  }
}
