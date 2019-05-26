import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by rasingh on 10/2/16.
 */
public class MyProducer {
    final static Logger logger = Logger.getLogger(MyProducer.class);

    public static void main(String[] args) throws IOException {
        if(args.length<1){
            System.out.println("usage : java -cp <>.jar MyProducer <topic>");
            System.exit(-1);
        }

        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try {
            int i=0;
            while(i < 100) {
                //ProducerRecord<String, String> record = new ProducerRecord<>(args[0], "value-" + i);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(args[0],"key-"+i,"value-"+i);
                System.out.println("Produced record ======= " + "key-"+i + "-->" +  "value-"+i);
                producer.send(record);
                i++;
                //Thread.sleep(1000);
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }

}
