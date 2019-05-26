package kafka

import java.io.File
import java.nio.ByteBuffer

import joptsimple.OptionParser
import kafka.coordinator.{GroupMetadataKey, GroupMetadataManager, OffsetKey}
import kafka.log.FileMessageSet
import kafka.message._
import kafka.serializer.Decoder
import kafka.utils.{CoreUtils, IteratorTemplate, VerifiableProperties}
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable

object KafkaTest {
  val parser = new OptionParser
  val filesOpt = parser.accepts("files", "REQUIRED: The comma separated list of data and index log files to be dumped")
    .withRequiredArg
    .describedAs("file1, file2, ...")
    .ofType(classOf[String])

  def getIterator(messageAndOffset: MessageAndOffset, isDeepIteration: Boolean) = {
    if (isDeepIteration) {
      val message = messageAndOffset.message
      message.compressionCodec match {
        case NoCompressionCodec =>
          getSingleMessageIterator(messageAndOffset)
        case _ =>
          ByteBufferMessageSet.deepIterator(messageAndOffset)
      }
    } else
      getSingleMessageIterator(messageAndOffset)
  }

  def getSingleMessageIterator(messageAndOffset: MessageAndOffset) = {
    new IteratorTemplate[MessageAndOffset] {
      var messageIterated = false

      override def makeNext(): MessageAndOffset = {
        if (!messageIterated) {
          messageIterated = true
          messageAndOffset
        } else
          allDone()
      }
    }
  }


  def executePrint(msg: Message) = {
    val (key, payload) = messageParser.parse(msg)
    key.map(key => print(s" key: ${key}"))
    payload.map(payload => print(s" payload: ${payload}"))
  }

  /* print out the contents of the log */
  def dumpLog(file: File,
              printContents: Boolean,
              nonConsecutivePairsForLogFilesMap: mutable.HashMap[String, List[(Long, Long)]],
              isDeepIteration: Boolean,
              maxMessageSize: Int,
              parser: MessageParser[_, _]) {
    val startOffset = file.getName().split("\\.")(0).toLong
    println("Starting offset: " + startOffset)
    val messageSet = new FileMessageSet(file, false)
    var validBytes = 0L
    var lastOffset = -1l
    val shallowIterator = messageSet.iterator(maxMessageSize)
    for (shallowMessageAndOffset <- shallowIterator) { // this only does shallow iteration
      val itr = getIterator(shallowMessageAndOffset, isDeepIteration)
      for (messageAndOffset <- itr) {
        val msg = messageAndOffset.message

        if (lastOffset == -1)
          lastOffset = messageAndOffset.offset
        // If we are iterating uncompressed messages, offsets must be consecutive
        else if (msg.compressionCodec == NoCompressionCodec && messageAndOffset.offset != lastOffset + 1) {
          var nonConsecutivePairsSeq = nonConsecutivePairsForLogFilesMap.getOrElse(file.getAbsolutePath, List[(Long, Long)]())
          nonConsecutivePairsSeq ::= (lastOffset, messageAndOffset.offset)
          nonConsecutivePairsForLogFilesMap.put(file.getAbsolutePath, nonConsecutivePairsSeq)
        }
        lastOffset = messageAndOffset.offset

        print("offset: " + messageAndOffset.offset + " position: " + validBytes + " isvalid: " + msg.isValid +
          " payloadsize: " + msg.payloadSize + " magic: " + msg.magic +
          " compresscodec: " + msg.compressionCodec + " crc: " + msg.checksum)
        if (msg.hasKey)
          print(" keysize: " + msg.keySize)
        if (printContents) {
          val (key, payload) = parser.parse(msg)
          key.map(key => print(s" key: ${key}"))
          payload.map(payload => print(s" payload: ${payload}"))
        }
        println()
      }
      validBytes += MessageSet.entrySize(shallowMessageAndOffset.message)
    }
    val trailingBytes = messageSet.sizeInBytes - validBytes
    if (trailingBytes > 0)
      println("Found %d invalid bytes at the end of %s".format(trailingBytes, file.getName))
  }

  val options = parser.parse()

  val valueDecoderOpt = parser.accepts("value-decoder-class", "if set, used to deserialize the messages. This class should implement kafka.serializer.Decoder trait. Custom jar should be available in kafka/libs directory.")
    .withOptionalArg()
    .ofType(classOf[java.lang.String])
    .defaultsTo("kafka.serializer.StringDecoder")
  val keyDecoderOpt = parser.accepts("key-decoder-class", "if set, used to deserialize the keys. This class should implement kafka.serializer.Decoder trait. Custom jar should be available in kafka/libs directory.")
    .withOptionalArg()
    .ofType(classOf[java.lang.String])
    .defaultsTo("kafka.serializer.StringDecoder")
  val offsetsOpt = parser.accepts("offsets-decoder", "if set, log data will be parsed as offset data from __consumer_offsets topic")

//  val messageParser = if (options.has(offsetsOpt)) {
//    new OffsetsMessageParser
//  } else {
//    val valueDecoder: Decoder[_] = CoreUtils.createObject[Decoder[_]]("kafka.serializer.StringDecoder", new VerifiableProperties)
//    val keyDecoder: Decoder[_] = CoreUtils.createObject[Decoder[_]]("kafka.serializer.StringDecoder", new VerifiableProperties)
//    new DecoderMessageParser(keyDecoder, valueDecoder)
//  }

  val messageParser = new OffsetsMessageParser
}

private class OffsetsMessageParser extends MessageParser[String, String] {
   def hex(bytes: Array[Byte]): String = {
    if (bytes.isEmpty)
      ""
    else
      String.format("%X", BigInt(1, bytes))
  }

  def parseOffsets(offsetKey: OffsetKey, payload: ByteBuffer) = {
    val group = offsetKey.key.group
    val topicPartition = offsetKey.key.topicPartition
    val offset = GroupMetadataManager.readOffsetMessageValue(payload)

    val keyString = s"offset::${group}:${topicPartition.topic}:${topicPartition.partition}"
    val valueString = if (offset.metadata.isEmpty)
      String.valueOf(offset.offset)
    else
      s"${offset.offset}:${offset.metadata}"

    (Some(keyString), Some(valueString))
  }

  def parseGroupMetadata(groupMetadataKey: GroupMetadataKey, payload: ByteBuffer) = {
    val groupId = groupMetadataKey.key
    val group = GroupMetadataManager.readGroupMessageValue(groupId, payload)
    val protocolType = group.protocolType

    val assignment = group.allMemberMetadata.map { member =>
      if (protocolType == ConsumerProtocol.PROTOCOL_TYPE) {
        val partitionAssignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment))
        val userData = hex(Utils.toArray(partitionAssignment.userData()))

        if (userData.isEmpty)
          s"${member.memberId}=${partitionAssignment.partitions()}"
        else
          s"${member.memberId}=${partitionAssignment.partitions()}:${userData}"
      } else {
        s"${member.memberId}=${hex(member.assignment)}"
      }
    }.mkString("{", ",", "}")

    val keyString = s"metadata::${groupId}"
    val valueString = s"${protocolType}:${group.protocol}:${group.generationId}:${assignment}"

    (Some(keyString), Some(valueString))
  }

  override def parse(message: Message): (Option[String], Option[String]) = {
    if (message.isNull)
      (None, None)
    else if (!message.hasKey) {
      throw new KafkaException("Failed to decode message using offset topic decoder (message had a missing key)")
    } else {
      GroupMetadataManager.readMessageKey(message.key) match {
        case offsetKey: OffsetKey => parseOffsets(offsetKey, message.payload)
        case groupMetadataKey: GroupMetadataKey => parseGroupMetadata(groupMetadataKey, message.payload)
        case _ => throw new KafkaException("Failed to decode message using offset topic decoder (message had an invalid key)")
      }
    }
  }
}

trait MessageParser[K, V] {
  def parse(message: Message): (Option[K], Option[V])
}

private class DecoderMessageParser[K, V](keyDecoder: Decoder[K], valueDecoder: Decoder[V]) extends MessageParser[K, V] {
  override def parse(message: Message): (Option[K], Option[V]) = {
    if (message.isNull) {
      (None, None)
    } else {
      val key = if (message.hasKey)
        Some(keyDecoder.fromBytes(Utils.readBytes(message.key)))
      else
        None

      val payload = Some(valueDecoder.fromBytes(Utils.readBytes(message.payload)))

      (key, payload)
    }
  }
}

