package com.manu.kafka.basic;

import com.manu.kafka.largemessages.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileFilter;
import java.util.Properties;

public class MyProducer_1MB {
    static long start = System.currentTimeMillis();

    public static void main(String s[]) {

        System.out.println("manu10");
        String filePath = "/Users/mvanam/Documents/kafkaTest/";
        String topic = "manu4";
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "c418-node2.squadron-labs.com:6667");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
//        props.put("security.protocol", "PLAINTEXT");

//        KafkaProducer<String, String> producer;
//        try (InputStream props = Resources.getResource("producer.props").openStream()) {
//            Properties properties = new Properties();
//            properties.load(props);
//            producer = new KafkaProducer<>(properties);
//        }

        start = System.currentTimeMillis();
        final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
File file = new File("/Users/mvanam/Documents/kafkaTest/mano.log");
        try {
//            File[] files = new File(filePath).listFiles(new FileFilter() {
//                @Override
//                public boolean accept(File pathname) {
//                    return pathname.getAbsolutePath().endsWith("log");
//                }
//            });
            byte[] value = FileUtils.readFileInList(file.getAbsolutePath());
            for (int i = 0; i < 10000; i++) {
                producer.send(new ProducerRecord(topic, value));
                System.out.println("Produced record ======= " + "key-" + i + "-->" + "value-" + i);
//                Thread.sleep(1000);
            }
            long end = System.currentTimeMillis();
            System.out.println("Total time : " + (end - start) + "MilliSecs");
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }
    }
}

