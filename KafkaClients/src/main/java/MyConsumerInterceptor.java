import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MyConsumerInterceptor implements ConsumerInterceptor{
    private static final Logger log = LoggerFactory.getLogger(MyConsumerInterceptor.class);
    @Override
    public ConsumerRecords onConsume(ConsumerRecords records) {
        log.debug("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | onConsume consumed : "+records.count()+ " records");
        return records;
    }

    @Override
    public void close() {
        log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | consumer is closed");

    }

    @Override
    public void onCommit(Map offsets) {
        for (Object entry : offsets.entrySet()) {
            Map.Entry<TopicPartition, OffsetAndMetadata> entry1 = (Map.Entry) entry;
            log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | onCommit topic : "
            +entry1.getKey().topic()+" , partition : "+entry1.getKey().partition()+" , offset and metadata : "+entry1.getValue().toString());
        }

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
