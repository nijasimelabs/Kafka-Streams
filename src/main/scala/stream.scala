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
    val trafficTopic: String = "traffic_info"
    val operationalSCPCTopic: String = "operational_scpc"

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
   val trafficStream: KStream[String, JsonNode] = builder.stream(trafficTopic, Consumed.`with`(stringSerde, jsonSerde));
   val new_stream = trafficStream
   // val trafficClassificationTable: KTable[String, JsonNode] = builder.table(trafficClassificationTopic, Consumed.`with`(stringSerde, jsonSerde))
   // val trafficClassTable: KTable[String, JsonNode] = builder.table(trafficClassTopic, Consumed.`with`(stringSerde, jsonSerde))

   // joiner for trafic class and traffic Shaping. join these two values by nodename, since the same key is present on both nodes
   // stream for only those records with matching nodename.
   // val joiner: ValueJoiner[JsonNode, JsonNode, JsonNode] = new ValueJoiner[JsonNode, JsonNode, JsonNode]() {
   //    def apply(traffic_shaping_obj: JsonNode, traffic_classification_obj: JsonNode):JsonNode={
   //       val jNode:ObjectNode = JsonNodeFactory.instance.objectNode();
   //       jNode.put("nodename", traffic_shaping_obj.path("nodename").asText()) 
   //       jNode.put("datetime",traffic_shaping_obj.path("datetime").asText())
   //       jNode.put("expression",traffic_classification_obj.path("expression").asText())
   //       jNode.put("cir", traffic_shaping_obj.path("cir").asText())
   //       jNode.put("mir", traffic_shaping_obj.path("mir").asText())
   //       jNode.put("link", traffic_shaping_obj.path("link").asText())
   //       jNode.put("classname", traffic_classification_obj.path("classname").asText())
   //       return jNode;
   //    }
   //  }



   //  // joiner trafficClass with traffic_shaping + traffic Classification

   //  val joiner2: ValueJoiner[JsonNode, JsonNode, JsonNode] = new ValueJoiner[JsonNode, JsonNode, JsonNode]() {
   //    def apply(trafficCombinedObj: JsonNode, trafficClassObj: JsonNode):JsonNode={
         
   //       val jNode:ObjectNode = JsonNodeFactory.instance.objectNode();
   //       jNode.put("nodename", trafficCombinedObj.path("nodename").asText()) 
   //       jNode.put("expression",trafficCombinedObj.path("expression").asText())
   //       jNode.put("cir", trafficCombinedObj.path("cir").asText())
   //       jNode.put("datetime",trafficCombinedObj.path("datetime").asText())
   //       jNode.put("mir", trafficCombinedObj.path("mir").asText())
   //       jNode.put("link", trafficCombinedObj.path("link").asText())
   //       jNode.put("classname", trafficCombinedObj.path("classname").asText())
   //       jNode.put("classname1", trafficClassObj.path("classname").asText())
   //       return jNode;
   //    }
   //  }        
    
   // val trafficJoined: KStream[String, JsonNode]  = trafficShapingStream.join(trafficClassificationTable,  joiner)
   // val trafficJoinedFiltered = trafficJoined.filter((key, jsonObj) => jsonObj.path("nodename").asText().equals(jsonObj.path("classname").asText()))
   // val trafficJoinedSecond: KStream[String, JsonNode]  = trafficJoinedFiltered.join(trafficClassTable,  joiner2)
   // val trafficJoinedSecondFiltered = trafficJoinedSecond.filter((key, jsonObj) => jsonObj.path("nodename").asText().equals(jsonObj.path("classname1").asText()))
   new_stream.to("out-topic", Produced.`with`(stringSerde, jsonSerde))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.start();

    
  }
}
