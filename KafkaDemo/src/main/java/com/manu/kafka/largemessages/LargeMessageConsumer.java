package com.manu.kafka.largemessages;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


// Usage: java -cp KafkaDemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.manu.kafka.largemessages.LargeMessageConsumer q3 /Users/mvanam/Downloads/output.log ../src/main/resources/consumer.props
public class LargeMessageConsumer {
    static long start = System.currentTimeMillis();
    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length != 3) {
            System.out.println("Missing arguments : " + "topic, file");
        }
        String topic = args[0];
        String filePath = args[1];
        String PropPath = args[2];
        LargeMessageConsumer consumer = new LargeMessageConsumer();
        start = System.currentTimeMillis();
        consumer.startConsumer(topic, filePath, PropPath);
//        System.out.println("Total time : " + (end - start) + "MilliSecs");

//        consumer.close();
    }

    private void startConsumer(String topic, String filePath, String propPath) throws IOException, InterruptedException {
        //make it singkleton
//        Map<String, Message> globalMap = new HashMap<>();
//        String topic = "t06";
        Consumer<String, byte[]> consumer;
        try (FileInputStream fis = new FileInputStream(propPath)) {
            Properties properties = new Properties();
            properties.load(fis);
            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(30000);
            for (ConsumerRecord<String, byte[]> record : records) {
//                        System.out.println(record);
//                Thread.sleep(1000);
                System.out.printf("Consumed record offset = %d, key = %s", record.offset(), record.key());

                assembleRecord(record.value(), filePath);
                System.out.println("\n");
            }
            consumer.commitSync();
        }
//        consumer.close();
    }


    private void assembleRecord(byte[] value, String filePath) {
        ByteBuffer bf = ByteBuffer.wrap(value);
        byte[] uuid = new byte[MessageConstants.UUID_BYTES];
        bf.get(uuid);
        String id = new String(uuid, MessageConstants.DEFAULT_CHARSET);
        Short index = bf.getShort();
        Short chunks = bf.getShort();
        System.out.println("uuid" + id);
        System.out.println("index " + index);
        System.out.println("chunks " + chunks);
        byte[] byteMsg = new byte[value.length - MessageConstants.UUID_BYTES - MessageConstants.SHORT_BYTES - MessageConstants.SHORT_BYTES];
        bf.get(byteMsg);
        String msg = new String(byteMsg, MessageConstants.DEFAULT_CHARSET);
        GlobalConsumerMap map = GlobalConsumerMap.getInstance();
        boolean isEntireMsgReceived = map.addMessage(id, byteMsg, index, chunks);
        if (isEntireMsgReceived) {
            FileUtils.writeFile(map.getMessage(id).getMessages(), filePath + "file-" + id +".log");
            System.out.println("Consumed Entire Message Successfully..");
            map.removeMessage(id);
            long end = System.currentTimeMillis();
            System.out.println("Total time : " + (end - start) + "MilliSecs");
        }

//        if (globalMap.get(id) == null) {
//            globalMap.put(id, new Message(byteMsg, index, chunks));
//        } else {
//            globalMap.get(id).addMessage(byteMsg, index, chunks);
//        }
//        if(isMessageConsumedCompletely()) {
//
//        }

//        System.out.println(globalMap);
    }
}

