import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;

public class KafkaConsumerApiDemo {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApiDemo.class);


    public static void main(String[] args) throws Exception {
        {
            if(args.length<1){
                System.out.println("usage : java -cp <>.jar <topic1> <topic2> ..");
                System.exit(-1);
            }

            List<String> topics = new ArrayList<>();
            for(String arg:args){
                topics.add(arg);
            }

            KafkaConsumer<String, String> consumer;
            try (InputStream props = Resources.getResource("consumer.props").openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                if (properties.getProperty("group.id") == null) {
                    properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
                }
                consumer = new KafkaConsumer<String, String>(properties);
            }
            //consumeMessages(consumer,topics);
            //seekAndConsumeMessage(consumer,"kafkatopic");
            //getNextOffsetToFetch(consumer,"kafkatopic");
            // Get the last committed offset for the given partition
            //getLastCommittedOffset(consumer,"Kafkatopic");
            //getKafkaConsumerMetrics(consumer);

            //Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it does not already have any metadata about the given topic.
            //getPartitionsForTopic(consumer,"kafkatopic");

            //Get metadata about partitions for all topics that the user is authorized to view. This method will issue a remote call to the server.
            //listTopics(consumer);

            //Get the first offset for the given partitions.
            //getBeginningOffsetForTopicPartition(consumer,"kafkatopic");

            //Get the last offset for the given partitions.
            //getEndOffsetForTopicPartition(consumer,"kafkatopic");

            // Look up the offsets for the given partitions by timestamp
            TopicPartition topicPartition0 = new TopicPartition("kafkatopic",0);
            Map<TopicPartition,Long> topicPartitionTimeMap = new HashMap<>();
            topicPartitionTimeMap.put(topicPartition0,1517534507887l);
            getOffsetsForTimes(consumer,topicPartitionTimeMap);
        }
    }

    private static void getOffsetsForTimes(KafkaConsumer<String, String> consumer, Map<TopicPartition,Long> map) {
        TopicPartition topicPartition = null;
        Long timestamp = null;
        for(Map.Entry entry : map.entrySet()){
            topicPartition = (TopicPartition) entry.getKey();
            timestamp = (Long)entry.getValue();
            System.out.println("topicPartition "+topicPartition+" timestamp "+timestamp);
            break;
        }
        consumer.assign(Collections.singletonList(topicPartition));
        Map<TopicPartition, OffsetAndTimestamp> outMap = consumer.offsetsForTimes(map);

        for(Map.Entry entry : outMap.entrySet()){
            topicPartition = (TopicPartition) entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = (OffsetAndTimestamp)entry.getValue();
            System.out.println("topicPartition "+topicPartition+" [offset and timestamp]: "+offsetAndTimestamp);
        }


    }

    private static void getBeginningOffsetForTopicPartition(KafkaConsumer<String, String> consumer, String kafkatopic) {
        TopicPartition topicPartition = new TopicPartition(kafkatopic,0);
        consumer.assign(Collections.singletonList(topicPartition));
        Map<TopicPartition,Long> topicPartitionOffsetMap = consumer.beginningOffsets(Collections.singletonList(topicPartition));
        for (Map.Entry entry : topicPartitionOffsetMap.entrySet()){
            System.out.println("TopicPartition : "+entry.getKey()+" beginning offset : "+entry.getValue());
        }
    }

    private static void getEndOffsetForTopicPartition(KafkaConsumer<String, String> consumer, String kafkatopic) {
        TopicPartition topicPartition = new TopicPartition(kafkatopic,0);
        consumer.assign(Collections.singletonList(topicPartition));
        Map<TopicPartition,Long> topicPartitionOffsetMap = consumer.endOffsets(Collections.singletonList(topicPartition));
        for (Map.Entry entry : topicPartitionOffsetMap.entrySet()){
            System.out.println("TopicPartition : "+entry.getKey()+" end offset : "+entry.getValue());
        }
    }

    private static void listTopics(KafkaConsumer<String, String> consumer) {
        Map<String,List<PartitionInfo>> listMap = consumer.listTopics();
        for (Map.Entry entry : listMap.entrySet()){
            System.out.println("Topic Name : "+entry.getKey()+" PartitionInfo : "+entry.getValue());
        }
    }

    private static void getPartitionsForTopic(KafkaConsumer consumer,String kafkatopic) {
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(kafkatopic);
        for (PartitionInfo partitionInfo: partitionInfoList) {
            logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | partitionInfo: "+partitionInfo);
        }

    }

    private static void getKafkaConsumerMetrics(KafkaConsumer consumer) {
        Map<MetricName,? extends Metric> metricNameMap = consumer.metrics();
        for (Map.Entry entry : metricNameMap.entrySet())
        {
            KafkaMetric kafkaMetric = (KafkaMetric) entry.getValue();
            System.out.println("MetricName: " + entry.getKey() + "; Metric value: " + kafkaMetric.value());
        }
    }

    public static void getLastCommittedOffset(KafkaConsumer consumer,String topicName){
        TopicPartition topicPartition0 = new TopicPartition(topicName,0);
        consumer.assign(Collections.singletonList(topicPartition0));
        //Get the last committed offset for the given partition
        OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition0);
        logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | offsetAndMetadata: "+offsetAndMetadata);
    }

    public static void getNextOffsetToFetch(KafkaConsumer consumer,String topicName){
        TopicPartition topicPartition0 = new TopicPartition(topicName,0);
        consumer.assign(Collections.singletonList(topicPartition0));
        //Get the offset of the next record that will be fetched (if a record with that offset exists).
        long nextOffsetToFetch = consumer.position(topicPartition0);
        logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | nextOffsetToFetch: "+nextOffsetToFetch);
    }

    public static void consumeMessages(KafkaConsumer consumer,List<String> topics) throws Exception{
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | offset = %d, key = %s, value = %s "
                        +record.offset()+","+record.key()+","+record.value());
            }
        }
    }

    public static void seekAndConsumeMessage(KafkaConsumer consumer,String topicName) throws Exception{
        TopicPartition topicPartition = new TopicPartition(topicName,0);
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition,568);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | offset = %d, key = %s, value = %s "
                        +record.offset()+","+record.key()+","+record.value());
            }
        }
    }

}
