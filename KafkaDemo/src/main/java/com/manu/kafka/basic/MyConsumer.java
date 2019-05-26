package com.manu.kafka.basic;

        import com.manu.kafka.largemessages.FileUtils;
        import org.apache.kafka.clients.consumer.*;
        import org.apache.kafka.clients.producer.KafkaProducer;
        import org.apache.kafka.common.serialization.StringDeserializer;

        import java.io.FileInputStream;
        import java.io.FileNotFoundException;
        import java.io.IOException;
        import java.util.*;


public class MyConsumer {

    static long start = System.currentTimeMillis();

    public static void main(String[] args) throws InterruptedException, IOException {
        String filePath = "/Users/mvanam/Documents/kafkaOutput/";
        String topic = "test";
        start = System.currentTimeMillis();
        Properties properties = new Properties();
        Consumer<String, String> consumer;
        try (FileInputStream fis = new FileInputStream("src/main/resources/consumer.props")) {

            properties.load(fis);
            consumer = new KafkaConsumer<>(properties);
        }
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "c418-node2:6667");
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        int i =0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1);
            for (ConsumerRecord<String, String> record : records) {
//                        System.out.println(record);
//                Thread.sleep(1000);
                        System.out.printf("Consumed record offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value().toString());
//                FileUtils.writeFile(record.value(), filePath + "file-" + i++ + ".log");
//                        System.out.println("\n");
                long end = System.currentTimeMillis();
                System.out.println(" Total time : " + (end - start) + "MilliSecs");
            }
            consumer.commitSync();

        }
//        consumer.close();
    }
}