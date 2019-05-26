import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Manohar
 *
 */
public class ManuProducer {

    public static void main(String [] args) throws InterruptedException {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "C318-node2.squadron-labs.com:6667");
//        props.put("security.protocol", "PLAINTEXT");

//        KafkaProducer<String, String> producer;
//        try (InputStream props = Resources.getResource("producer.props").openStream()) {
//            Properties properties = new Properties();
//            properties.load(props);
//            producer = new KafkaProducer<>(properties);
//        }

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());


        for ( int i = 0; i < 1000000; i++ ) {
//            producer.send(new ProducerRecord("test1","" + i));
            producer.send(new ProducerRecord("sourceTridentTopic1","key"+i,"value" + i));
            System.out.println("Produced record ======= " + "key"+i + "-->" +  "value" + i);

//            Thread.sleep( 1000);
        }
        producer.close();
    }

}
