import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by rasingh on 10/2/16.
 */
public class MyProducerWithCallBack {

    final static Logger logger = Logger.getLogger(MyProducerWithCallBack.class);

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
            while(true) {
                //ProducerRecord<String, String> record = new ProducerRecord<>(args[0], "value-" + i);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(args[0],"key-"+i,"value-"+i);
                long start = System.nanoTime();
                logger.info("about to send message to topic : "+args[0]+" : key-"+i+": value-"+i+" : "+start);
                producer.send(record,new ProducerCallBack(start,args[0],"key-"+i,"value-"+i));
                long end = System.nanoTime();
                logger.info("time taken to send 1 message to topic : "+args[0]+" : key-"+i+": value-"+i+" : "+end+"  time taken "+(end-start));
                i++;
                Thread.sleep(3000);
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }

    private static class ProducerCallBack implements org.apache.kafka.clients.producer.Callback {
        private final long start;
        private final String topic;
        private final String key;
        private final String value;

        ProducerCallBack(long start, String topic,String key,String value){
            this.start=start;
            this.key=key;
            this.value=value;
            this.topic=topic;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            logger.info("callback complete for topic "+topic+" : "+"for key "+key+" : value "+value+" : "+ (System.nanoTime()-start));
            //logger.info("metadata :"+metadata.topic()+metadata.offset()+metadata.partition());
        }
    }
}
