import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by rasingh on 10/3/16.
 * this one is configure a JAAS config(jaas-client) with userKeyTab
 * set to false and useTicketCache to true, so that the privileges of the current users are being used.
 * e.g.
 * java -Djava.security.auth.login.config=/home/user/jaas-client.conf \
 * -Djava.security.krb5.conf=/etc/krb5.conf \
 * -Djavax.security.auth.useSubjectCredsOnly=false \
 * -cp KafkaClientsTest-jar-with-dependencies.jar \
 * MyProducerSecure rk1.hdp:6667 <topic>
 *
 * using keytab to login
 *
 * java -Djava.security.auth.login.config=/home/user/jaas_client_with_keytab.conf \
 * -Djava.security.krb5.conf=/etc/krb5.conf \
 * -Djavax.security.auth.useSubjectCredsOnly=true \
 * -cp KafkaClientsTest-jar-with-dependencies.jar \
 * MyProducerSecure  <topic>
 *
 */
public class MyProducerSecure {

    final static Logger logger = Logger.getLogger(MyProducerSecure.class);

    public static void main(String[] args) throws IOException {
        if(args.length<1){
            System.out.println("usage : java -cp <>.jar MyProducerSecure <topic>");
            System.exit(-1);
        }


        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            properties.put("security.protocol", "PLAINTEXTSASL");
            producer = new KafkaProducer<>(properties);
        }

        try {
            int i=0;
            while (true) {
                //ProducerRecord<String, String> record = new ProducerRecord<>(args[0], "value-" + i);
              //  ProducerRecord<String, String> record = new ProducerRecord<String, String>(args[0],"key-"+i,"value-"+i);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(args[0],"key-"+i,"value-"+ readFileInList("producer.props"));
                System.out.println("Produced record ======= " + "key-"+i + "-->" +  "value-"+i);
                producer.send(record);
                i++;
                Thread.sleep(100);
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }

    public static List<String> readFileInList(String fileName) {
        List<String> lines = Collections.emptyList();
        try {
            lines = Files.readAllLines(Paths.get("/usr/log.log"), StandardCharsets.UTF_8);
//            System.out.println(lines.toString());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return lines;
    }
}
