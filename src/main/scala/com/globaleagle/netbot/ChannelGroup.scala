package com.globaleagle.netbot

import collection.mutable.Map
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode, ArrayNode, TextNode};
import java.util.Properties
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.serialization._
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.ValueJoiner
import org.apache.kafka.streams.kstream.{Printed, KStream, KTable, Produced, Serialized, ForeachAction}
import scala.collection.JavaConverters._

import Constants._

object ChannelGroup extends App {

  def getAppName(): String = {
    return PROFILE_APP_ID
  }

  /**
   * Main method
   */
  def main(args: Array[String]): Unit = {

    val stringSerde: Serde[String] = Serdes.String()
    val jsonSerializer: Serializer[JsonNode] = new JsonSerializer()
    val jsonDeserializer: Deserializer[JsonNode] = new JsonDeserializer()
    val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)


    val config = getAppProperties() match {
      case Some(props) => props;
      case None => {
        throw new Exception("Could not read application properties")
      }
    }

    val props = getProducerProperties() match {
      case Some(props) => props;
      case None => {
        throw new Exception("Could not read producer properties")
      }
    }

    val producer = new KafkaProducer[String, String](props)

    val builder: StreamsBuilder = new StreamsBuilder()
    val aggregate_values: ObjectNode = JsonNodeFactory.instance.objectNode();
    var wandbstore = Map[String, Array[String]]()
    val store: ObjectNode = JsonNodeFactory.instance.objectNode();


    //define stream here
    val operationalStream: KStream[String, JsonNode] = builder.stream(TOPIC_OPSCPC, Consumed.`with`(stringSerde, jsonSerde))
    val wandbStream: KStream[String, JsonNode] = builder.stream(TOPIC_WANOPDB, Consumed.`with`(stringSerde, jsonSerde))
    val trafficStream: KStream[String, JsonNode] = builder.stream(TOPIC_TRAFFIC, Consumed.`with`(stringSerde, jsonSerde));


    operationalStream.foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {
          store.set(KEY_OPSCPC, value)
          var sum_ip_rate:Double = 0;
          for (i <- 0 until value.size()){
            sum_ip_rate = sum_ip_rate+ value.get(0).get("avaiableIPrate").asDouble()
          }
          aggregate_values.set("Total_available_iprate", JsonNodeFactory.instance.numberNode(sum_ip_rate))
        }
      }
    );


    wandbStream.foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {
          val wanlinks = value.get("links")
          for(wlink <- wanlinks.elements.asScala) {
            val linkName = wlink.get("key").asText
            var dscps = Array[String]()
            for(item <- wlink.get("dscp_value").elements.asScala) {
              dscps = dscps :+ item.asText
            }
            wandbstore = wandbstore + (linkName -> dscps)
          }
        }
      }
    );


    // processing the stream for aggregation
    trafficStream.foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {

          for( linkNode <- value.elements.asScala) {

            // calculate agrgegate cir, mir for each link
            // cir should be the sum of cir and max of mir
            var max_mir:Double = 0.0;
            var sum_cir:Double = 0.0;
            var link_name = linkNode.get("link").asText();
            val link:ObjectNode = JsonNodeFactory.instance.objectNode();

            val trafficClasses = linkNode.get("trafficclass")
            val dscpValues = wandbstore.getOrElse(link_name, Array[String]())
            val filteredClasses = trafficClasses.elements.asScala.filter(
              tclass => {
                dscpValues.exists(i => tclass.get("expression").asText().endsWith("dscp " + i))
              }
            )
            val updatedTrafficClasses = JsonNodeFactory.instance.arrayNode()
            for(item  <- filteredClasses) {
              updatedTrafficClasses.add(item)
            }

            // update trafficClasses
            linkNode.asInstanceOf[ObjectNode].set("trafficclass", updatedTrafficClasses)

            for (tclass <- linkNode.get("trafficclass").elements.asScala) {
              val cir = tclass.get("cir").asDouble();
              val mir = tclass.get("mir").asDouble()
              sum_cir = sum_cir + cir
              if (max_mir < mir){
                max_mir = mir;
              }
            }

            // json node for cir mir aggregate
            link.put("cir", sum_cir * 1000000)
            link.put("mir", max_mir * 1000000)
            aggregate_values.set(link_name, link)
          }

          val rootNode = JsonNodeFactory.instance.objectNode()
          val profiles = JsonNodeFactory.instance.arrayNode()
          val result: ObjectNode = JsonNodeFactory.instance.objectNode()
          // FIXME: check if required
          result.set(KEY_SPOKE, value.get(KEY_REMOTE))
          val channelGroups: ArrayNode = JsonNodeFactory.instance.arrayNode();

          //looping again to push the data to final data structure
          for(linkNode <- value.elements.asScala){

            val linkName = linkNode.get("link").asText();
            val channel = JsonNodeFactory.instance.objectNode();
            val groupname: TextNode = JsonNodeFactory.instance.textNode(linkName);
            channel.set(KEY_GROUP_NAME, groupname);
            val cir = List(aggregate_values.get(linkName).get("cir").asDouble(), channelIpRate(store.get(KEY_OPSCPC), linkName)).min
            val mir = List(aggregate_values.get(linkName).get("mir").asDouble(), channelIpRate(store.get(KEY_OPSCPC), linkName)).min
            channel.put("cir", cir);
            channel.put("mir", mir);
            val members = JsonNodeFactory.instance.arrayNode()

            for (tclass <- linkNode.get("trafficclass").elements.asScala){

              val channelObj =  JsonNodeFactory.instance.objectNode();
              val channelName: TextNode = JsonNodeFactory.instance.textNode(tclass.get("name").asText());
              channelObj.set("chan_name", channelName)

              val weight = calculateWeight(
                tclass.get("cir").asDouble() * 1000000,
                aggregate_values.get(linkName).get("cir").asDouble(),
                aggregate_values.get("Total_available_iprate").asDouble(),
                channelIpRate(store.get(KEY_OPSCPC), linkName)
              )

              channelObj.put("weight", weight)
              channelObj.put("mir", tclass.get("mir").asDouble())
              members.add(channelObj);
            }
            channel.set("channel_group", members);
            channelGroups.add(channel)

          }

          result.set(KEY_CHANNEL_GROUPS, channelGroups)
          profiles.add(result)
          rootNode.set(KEY_CHANNEL_GROUP_PROFILE, profiles)
          val data = new ProducerRecord[String, String](PROFILE_RESULT_TOPIC, PROFILE_RESULT_TOPIC_KEY, rootNode.toString())
          producer.send(data)

        }
      });


    val streamApp : KafkaStreams = new KafkaStreams(builder.build(), config)
    streamApp.start();
  }



  def calculateWeight(channel_cir:Double, aggregate_cir:Double, total_ip_rate:Double, channel_iprate:Double):Double={
    val weight =  (channel_cir/aggregate_cir)*(total_ip_rate/channel_iprate)*100;
    return weight;
  }

  def channelIpRate(operationalScpc:JsonNode, linkName:String):Double={
    var iprate:Double = 0.0;
    for ( i <- 0 until operationalScpc.size()){
      if(operationalScpc.get(i).get("linkname").asText().equals(linkName)){
        iprate = operationalScpc.get(i).get("avaiableIPrate").asDouble();
      }
    }

    return iprate;
  }

}
