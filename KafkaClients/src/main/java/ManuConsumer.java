import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by bjrke on 13.07.17.
 */
public class ManuConsumer {
    public static void main(String[] args) throws InterruptedException {
        for (;;) {
            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "C118-node2.squadron-labs.com:6668");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup2");
            props.put("security.protocol", "PLAINTEXT");
            Consumer<String, String> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());

            consumer.subscribe(Arrays.asList("sourceTridentTopic1"));
            for (int i =0;i<100 ; i++ ) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                if (i==i) {
                    for (ConsumerRecord<String, String> record : records) {
//                        System.out.println(record);
//                        Thread.sleep(1000);
                        System.out.printf("Consumed record offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                        System.out.println("\n");
                    }
                }
                consumer.commitSync();
            }
            consumer.close();
        }
    }
}
