import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;


public class MyConsumer {
    final static Logger logger = Logger.getLogger(MyConsumer.class);

    public static void main(String[] args) throws Exception {
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
                properties.setProperty("group.id", "group-" + new Random().nextInt());
            }
            consumer = new KafkaConsumer<String, String>(properties);
        }


        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println("\n");
                logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | offset = %d, key = %s, value = %s "
                        +record.offset()+","+record.key()+","+record.value());
            }
            logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | sleeping for : "+10000);
            Thread.sleep(10000);
            logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | wake up");
        }
    }
}