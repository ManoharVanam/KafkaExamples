package com.manu.kafka.largemessages;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// Usage : java -cp KafkaDemo-1.0-SNAPSHOT-jar-with-dependencies.jar com.manu.kafka.largemessages.LargeMessageProducer q3 /Users/mvanam/Downloads/log.log ../src/main/resources/producer.props
public class LargeMessageProducer {
    static long start = System.currentTimeMillis();

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Missing arguments : " + "topic, file");
        }
        String topic = args[0];
        String filePath = args[1];
        String propPath = args[2];
        LargeMessageProducer producer = new LargeMessageProducer();
        start = System.currentTimeMillis();
        producer.startProducer(topic, filePath, propPath);

    }

    private void startProducer(String topic, String filePath, String propPath) throws IOException {
        int chunkSize = 1 * 1024 * 1024;
        final KafkaProducer<String, String> producer;

        try (FileInputStream fis = new FileInputStream(propPath)) {
            Properties properties = new Properties();
            properties.load(fis);
            producer = new KafkaProducer<>(properties);
        }


        try {
            List<RecordMetadata> dataResult = new ArrayList<>();
            File[] files = new File(filePath).listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getAbsolutePath().endsWith("log");
                }
            });
            for (int i = 0; i < files.length; i++) {
//            byte[] valueBytes = readFileInList("/Users/mvanam/Downloads/log.log");
                byte[] valueBytes = FileUtils.readFileInList(files[i].getAbsolutePath());
                List<byte[]> chunks = generateChunks(valueBytes, chunkSize);
                List<Future<RecordMetadata>> recordResults = new ArrayList<>(chunks.size());
                for (int j = 0; j < chunks.size(); j++) {
                    Future<RecordMetadata> future = producer.send(new ProducerRecord(topic, "" + i, chunks.get(j)));
                    recordResults.add(future);
                    System.out.println("Produced record ======= " + "key-" + i + "-->" + "value" + chunks.get(j));
//                    Thread.sleep(1000);
                }

                dataResult.add(generateStatus(recordResults));
                for (RecordMetadata rm : dataResult) {
                    System.out.println("Partition : " + rm.partition() + " Min Offset: " + rm.offset());
                }
            }
            long end = System.currentTimeMillis();
            System.out.println("produced "+ files.length + "files successfully...");
            System.out.println("Total time : " + (end - start) + "MilliSecs");

        } catch (Throwable throwable) {
            System.out.printf("ERROR " + throwable.getStackTrace());
        } finally {
            producer.close();
        }
    }

    private RecordMetadata generateStatus(List<Future<RecordMetadata>> results) {

        List<RecordMetadata> rmList = new ArrayList<>(results.size());
        for (Future<RecordMetadata> future : results) {
            RecordMetadata rm = null;
            try {
                rm = future.get(1000, TimeUnit.MILLISECONDS);
                System.out.println("Partition : " + rm.partition() + " Offset: " + rm.offset());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }
            rmList.add(rm);
        }
        return Collections.min(rmList, new RecordMetadataComparator());

    }

    private List<byte[]> generateChunks(byte[] value, int chunkSize) {
        int loadSize = value.length;
        int chunkCount = (loadSize / chunkSize) + (loadSize % chunkSize == 0 ? 0 : 1);
        byte[] uuid = UUID.randomUUID().toString().getBytes(MessageConstants.DEFAULT_CHARSET);
        int start = 0;
        int index = 0;
        List<byte[]> subArrays = new ArrayList<>(chunkCount);
        while (start < loadSize) {
            int end = (start + chunkSize) < loadSize ? start + chunkSize : loadSize;
            int size = end - start;
            int headerSize = uuid.length + MessageConstants.SHORT_BYTES + MessageConstants.SHORT_BYTES;
            byte[] header = new byte[headerSize];

            byte[] chunk = new byte[headerSize + size];
            ByteBuffer bb = ByteBuffer.wrap(chunk);
            bb.put(uuid);
            bb.putShort((short) index);
            bb.putShort((short) chunkCount);

            System.arraycopy(value, start, chunk, headerSize, size);
            subArrays.add(chunk);
            index++;
            start = end;

        }
        return subArrays;

    }

    class RecordMetadataComparator implements Comparator<RecordMetadata> {

        @Override
        public int compare(RecordMetadata o1, RecordMetadata o2) {
            return Long.compare(o1.offset(), o2.offset());
        }
    }
}
