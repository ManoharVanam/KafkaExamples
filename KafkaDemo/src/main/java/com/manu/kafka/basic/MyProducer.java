package com.manu.kafka.basic;

import com.manu.kafka.largemessages.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileFilter;
import java.util.Properties;

public class MyProducer {
    static long start = System.currentTimeMillis();

    public static void main(String s[]) {

        System.out.println("manu10");
        String filePath = "/Users/mvanam/Documents/kafkaTest/";
        String topic = "test_0_10_old_2";
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "c418-node2.hdp-labs.com:6667");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("client.id", "testProd");

//        KafkaProducer<String, String> producer;
//        try (InputStream props = Resources.getResource("producer.props").openStream()) {
//            Properties properties = new Properties();
//            properties.load(props);
//            producer = new KafkaProducer<>(properties);
//        }

        start = System.currentTimeMillis();
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

        try {
            File[] files = new File(filePath).listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getAbsolutePath().endsWith("log");
                }
            });
            for (int i = 0; i < files.length; i++) {
                producer.send(new ProducerRecord(topic, "" + i, "" + "old-new1.0-secondtime" + i));
                System.out.println("Produced record ======= " + "key-" + i + "-->" + "value-" + FileUtils.readFileInList(files[i].getAbsolutePath()));
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

