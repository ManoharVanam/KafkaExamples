package com.manu.kafka.basic;

import kafka.coordinator.GroupMetadataManager;
import kafka.coordinator.OffsetKey;
import org.apache.kafka.clients.consumer.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerOffsetsConsumer {

    static long start = System.currentTimeMillis();

    public static void main(String[] args) throws InterruptedException, IOException {
        if(args.length != 3) {
            System.out.println("Missing Arguments topic groupId broker:port");
        }
        String topic = args[0];
        String groupId = args[1];
        String bootstrap_servers = args[2];
        String filePath = "/Users/mvanam/Documents/kafkaOutput/";
//        String topic = "__consumer_offsets";
//        String groupId = "hosp_car_avail_edifact_converter_1p_qa";
        start = System.currentTimeMillis();
        Properties properties = new Properties();
        Consumer<byte[], byte[]> consumer;
//        try (FileInputStream fis = new FileInputStream("src/main/resources/consumer.props")) {

//            properties.load(fis);
//            consumer = new KafkaConsumer<>(properties);
//        }
        properties.put("bootstrap.servers", bootstrap_servers );
        properties.put("group.id", "thisConsumer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        int i =0;
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(1);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                        System.out.println(record);
                byte[] key = record.key();
                Object obj =  GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key));
                if (obj != null && obj instanceof OffsetKey) {
                    OffsetKey offsetKey =  (OffsetKey) obj;
                    String value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value())).toString();
                    if(offsetKey.key().group().contains(groupId)) {
                        System.out.printf("Consumed record partiton=%d offset = %d, key = %s, value = %s", record.partition(), record.offset(), offsetKey.key(), value);
                        System.out.println();
                    }
                }
            }
            consumer.commitSync();

        }
//        consumer.close();
    }
}