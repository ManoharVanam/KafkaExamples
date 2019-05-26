import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MyProducerInterceptor implements ProducerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(MyProducerInterceptor.class);
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        log.debug("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | onSend topic :"+
        record.topic()+" : key: "+record.key()+" : value: "+record.value()+" partition : "+record.partition()+" timestamp : "+record.timestamp());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        log.debug("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | onAcknowledgement topic :"+
                metadata.topic()+" : partition: "+metadata.partition()+" : offset: "+metadata.offset()+" timestamp : "+metadata.timestamp());
    }

    @Override
    public void close() {
        log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | producer is closed");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
