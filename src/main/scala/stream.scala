import java.util.Properties
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{Printed, KStream, KTable, Produced, Serialized}
import org.apache.kafka.streams.kstream.ValueJoiner
import org.apache.kafka.streams._



/**
  * Copyright Knoldus Software LLP, 2017. All rights reserved.
  */
object ChannelGroupProfileStream {
  def main(args: Array[String]): Unit = {
    
    val wanOperationalDbTopic: String = "wan_op_db"
    val trafficShapingTopic: String ="traffic_shaping"
    val trafficClassTopic: String = "traffic_class"
    val trafficClassificationTopic: String = "traffic_Classification"
    val operationalSCPCTopic: String = "operational_scpc"
    val remoteTopic: String = "remote"

    val stringSerde: Serde[String] = Serdes.String()
    val jsonSerializer: Serializer[JsonNode] = new JsonSerializer()
    val jsonDeserializer: Deserializer[JsonNode] = new JsonDeserializer()
    val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)


    val config = {
      val properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application")
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      properties
    }

    val builder: StreamsBuilder = new StreamsBuilder()

   
    val trafficShapingStream: KStream[String, JsonNode] = builder.stream(trafficShapingTopic, Consumed.`with`(stringSerde, jsonSerde));

    val trafficClassificationTable: KTable[String, JsonNode] = builder.table(trafficClassificationTopic, Consumed.`with`(stringSerde, jsonSerde))

    val joiner: ValueJoiner[JsonNode, JsonNode, String] = new ValueJoiner[JsonNode, JsonNode, String]() {
      def apply(a: JsonNode, b: JsonNode): String = a.path("nodename").asText()
    }    
    
    val joined: KStream[String, String]  = trafficShapingStream.leftJoin(trafficClassificationTable,  joiner)
    

    joined.to("out-topic", Produced.`with`(stringSerde, stringSerde))
    
    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.start();

    
  }
}
