package kafka.tools

import java.util.{Locale, Properties}

import joptsimple._
import kafka.tools.CustomGetOffsetShell.isSaslProtocol
import kafka.utils.{CommandLineUtils, Exit, ToolsUtils}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._

object GetOffsetCode
{

  def main(args: Array[String]): Unit = {

    val clientId = "GetOffsetShell"
    val topic = "mano"
    val bootstrapServer = "c127-node2:6667"
    val securityProtocol = "PLAINTEXT"

    val listOffsetsTimestamp = -1L
    val partitionIdsRequested: Set[Int] = Set.empty

    val config = new Properties
    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    config.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

    if (isSaslProtocol(securityProtocol)) {
      config.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.toString);
    }
    else {
      config.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);
    }
    val consumer = new KafkaConsumer(config, new ByteArrayDeserializer, new ByteArrayDeserializer)

    val partitionInfos = listPartitionInfos(consumer, topic, partitionIdsRequested) match {
      case None =>
        System.err.println(s"Topic $topic does not exist")
        Exit.exit(1)
      case Some(p) if p.isEmpty =>
        if (partitionIdsRequested.isEmpty)
          System.err.println(s"Topic $topic has 0 partitions")
        else
          System.err.println(s"Topic $topic does not have any of the requested partitions ${partitionIdsRequested.mkString(",")}")
        Exit.exit(1)
      case Some(p) => p
    }

    if (partitionIdsRequested.nonEmpty) {
      (partitionIdsRequested -- partitionInfos.map(_.partition)).foreach { partitionId =>
        System.err.println(s"Error: partition $partitionId does not exist")
      }
    }

    val topicPartitions = partitionInfos.sortBy(_.partition).flatMap { p =>
      if (p.leader == null) {
        System.err.println(s"Error: partition ${p.partition} does not have a leader. Skip getting offsets")
        None
      } else
        Some(new TopicPartition(p.topic, p.partition))
    }

    /* Note that the value of the map can be null */
    val partitionOffsets: collection.Map[TopicPartition, java.lang.Long] = listOffsetsTimestamp match {
      case ListOffsetRequest.EARLIEST_TIMESTAMP => consumer.beginningOffsets(topicPartitions.asJava).asScala
      case ListOffsetRequest.LATEST_TIMESTAMP => consumer.endOffsets(topicPartitions.asJava).asScala
      case _ =>
        val timestampsToSearch = topicPartitions.map(tp => tp -> (listOffsetsTimestamp: java.lang.Long)).toMap.asJava
        consumer.offsetsForTimes(timestampsToSearch).asScala.mapValues(x => if (x == null) null else x.offset)
    }

    partitionOffsets.toSeq.sortBy { case (tp, _) => tp.partition }.foreach { case (tp, offset) =>
      println(s"$topic:${tp.partition}:${Option(offset).getOrElse("")}")
    }

  }

  /**
    * Return the partition infos for `topic`. If the topic does not exist, `None` is returned.
    */
  private def listPartitionInfos(consumer: KafkaConsumer[_, _], topic: String, partitionIds: Set[Int]): Option[Seq[PartitionInfo]] = {
    val partitionInfos = consumer.listTopics.asScala.filterKeys(_ == topic).values.flatMap(_.asScala).toBuffer
    if (partitionInfos.isEmpty)
      None
    else if (partitionIds.isEmpty)
      Some(partitionInfos)
    else
      Some(partitionInfos.filter(p => partitionIds.contains(p.partition)))
  }

    def isSaslProtocol(protocol: String): Boolean = {
      protocol == SecurityProtocol.SASL_PLAINTEXT.toString || protocol == SecurityProtocol.SASL_SSL.toString || protocol == SecurityProtocol.SSL.toString
      // protocol == SecurityProtocol.SASL_PLAINTEXT || protocol == SecurityProtocol.PLAINTEXTSASL
    }

}