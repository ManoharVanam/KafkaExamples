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

/**
 * Created by rasingh on 10/3/16.
 * this one is configure a JAAS config(jaas-client) with userKeyTab
 * set to false and useTicketCache to true, so that the privileges of the current users are being used.
 * e.g.
 * java -Djava.security.auth.login.config=/home/user/jaas-client.conf \
 * -Djava.security.krb5.conf=/etc/krb5.conf \
 * -Djavax.security.auth.useSubjectCredsOnly=false \
 * -cp KafkaClientsTest-jar-with-dependencies.jar \
 * MyConsumerSecure rk1.hdp:6667 <topic>
 *
 * using keytab to login
 *
 * java -Djava.security.auth.login.config=/home/user/jaas_client_with_keytab.conf \
 * -Djava.security.krb5.conf=/etc/krb5.conf \
 * -Djavax.security.auth.useSubjectCredsOnly=true \
 * -cp KafkaClientsTest-jar-with-dependencies.jar \
 * MyConsumerSecure  <topic>
 */
public class MyConsumerSecure {

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
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            properties.put("security.protocol", "PLAINTEXTSASL");
            consumer = new KafkaConsumer<String, String>(properties);
        }


        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            System.out.println("Records count + " + records.count());
            for (ConsumerRecord<String, String> record : records){
                System.out.println("Record value count " + record.value().length());
//                System.out.printf("Consumed record offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.printf("Consumed record offset = %d, key = %s, value = %s", record.offset(), record.key(), record.key());
                System.out.println("\n");
            }

        }
    }
}
