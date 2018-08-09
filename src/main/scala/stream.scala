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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;



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
   //define stream here 
   val trafficShapingStream: KStream[String, JsonNode] = builder.stream(trafficShapingTopic, Consumed.`with`(stringSerde, jsonSerde));
   val trafficClassificationTable: KTable[String, JsonNode] = builder.table(trafficClassificationTopic, Consumed.`with`(stringSerde, jsonSerde))
   val trafficClassTable: KTable[String, JsonNode] = builder.table(trafficClassTopic, Consumed.`with`(stringSerde, jsonSerde))

   // joiner for trafic class and traffic Shaping. join these two values by nodename, since the same key is present on both nodes
   val joiner: ValueJoiner[JsonNode, JsonNode, JsonNode] = new ValueJoiner[JsonNode, JsonNode, JsonNode]() {
      def apply(traffic_shaping_obj: JsonNode, traffic_classification_obj: JsonNode):JsonNode={
         
         val jNode:ObjectNode = JsonNodeFactory.instance.objectNode();
         jNode.put("nodename", traffic_shaping_obj.path("nodename").asText()) 
         jNode.put("datetime",traffic_shaping_obj.path("datetime").asText())
         jNode.put("expression",traffic_classification_obj.path("expression").asText())
         jNode.put("cir", traffic_shaping_obj.path("cir").asText())
         jNode.put("mir", traffic_shaping_obj.path("mir").asText())
         jNode.put("link", traffic_shaping_obj.path("link").asText())
         jNode.put("classname", traffic_shaping_obj.path("nodename").asText())
         return jNode;
      }
    }



    // joiner trafficClass with traffic_shaping + traffic Classification

    val joiner2: ValueJoiner[JsonNode, JsonNode, JsonNode] = new ValueJoiner[JsonNode, JsonNode, JsonNode]() {
      def apply(traffic_combined_obj: JsonNode, traffic_class_obj: JsonNode):JsonNode={
         
         val jNode:ObjectNode = JsonNodeFactory.instance.objectNode();
         jNode.put("nodename", traffic_combined_obj.path("nodename").asText()) 
         jNode.put("expression",traffic_combined_obj.path("expression").asText())
         jNode.put("cir", traffic_combined_obj.path("cir").asText())
         jNode.put("datetime",traffic_combined_obj.path("datetime").asText())
         jNode.put("mir", traffic_combined_obj.path("mir").asText())
         jNode.put("link", traffic_combined_obj.path("link").asText())
         jNode.put("classname", traffic_combined_obj.path("nodename").asText())
         return jNode;
      }
    }        
    
    val traffic_joined: KStream[String, JsonNode]  = trafficShapingStream.join(trafficClassificationTable,  joiner)
    val traffic_joined2: KStream[String, JsonNode]  = traffic_joined.join(trafficClassTable,  joiner2)
    
    traffic_joined2.to("out-topic", Produced.`with`(stringSerde, jsonSerde))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.start();

    
  }
}
