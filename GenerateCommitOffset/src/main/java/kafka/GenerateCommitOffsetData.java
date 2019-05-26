package kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.log.FileMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.TopicPartition;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.exit;

public class GenerateCommitOffsetData {

    private static final Map<String, Map<TopicPartition, Long>> offsetsMap = new HashMap<>();

    public static void main(String[] args) throws JsonProcessingException {
        if (args.length != 1) {
            System.out.println("Usage: java -cp <jar-path> kafka.GenerateCommitOffsetData <kafka-data-dir-path>");
            exit(0);
        }
        String dirPath = args[0];
        final File dir = new File(dirPath);
        final File[] files = dir.listFiles((dir1, name) -> name.startsWith("__consumer_offsets"));
        for (final File file : files) {
            System.out.println("Parsing the file : " + file.getName());
            final File[] logFiles = file.listFiles((dir12, name) -> name.endsWith(".log"));
            Arrays.sort(logFiles, Comparator.comparingLong(a -> Long.parseLong(getSimpleFileName(a.getName()).split(".log")[0])));
            for (final File lf : logFiles) {
                execute(lf.getAbsolutePath());
            }
        }

        final ObjectMapper mapper = new ObjectMapper();
        final String jsonOffsetsMap = mapper.writeValueAsString(offsetsMap);
        System.out.println("****JSON Content START*****");
        System.out.println(jsonOffsetsMap);
        System.out.println("****JSON Content END*****");
        System.out.println("Completed Successfully");

    }

    private static String getSimpleFileName(final String fileName) {
        final int index = fileName.lastIndexOf('/');
        if (index == -1) {
            return fileName;
        } else {
            return fileName.substring(index + 1);
        }
    }

    private static void execute(String file) {

        boolean isDeepIteration = true;
        long validBytes = 0L;
        FileMessageSet messageSet = new FileMessageSet(new File(file), true);
        Iterator<MessageAndOffset> shallowIterator = messageSet.iterator();
        while (shallowIterator.hasNext()) {
            MessageAndOffset shallowMessageAndOffset = shallowIterator.next();
            Iterator<MessageAndOffset> itr = DumLogSegments.getIterator(shallowMessageAndOffset, isDeepIteration);
            while (itr.hasNext()) {
                MessageAndOffset messageAndOffset = itr.next();
                Message msg = messageAndOffset.message();

                // print the below line in debug mode.
//                System.out.println("offset: " + messageAndOffset.offset() + " position: " + validBytes + " isvalid: " + msg.isValid() +
//                        " payloadsize: " + msg.payloadSize() + " magic: " + msg.magic() +
//                        " compresscodec: " + msg.compressionCodec() + " crc: " + msg.checksum());


                final Tuple2<String, String> t2 = DumLogSegments.parseMsg(msg);
                final String key = t2._1();
                final String payload = t2._2();

                if (key.startsWith("offset")) {
                    // KEY => offset::console-consumer-49318:test:1
                    // PAYLOAD => 2
                    final String[] chunks = key.split(":");
                    final String groupId = chunks[2];
                    final TopicPartition tp = new TopicPartition(chunks[3], Integer.parseInt(chunks[4]));
                    final long lastCommittedOffset = Long.parseLong(payload);

                    final Map<TopicPartition, Long> offsetsByTp = offsetsMap.computeIfAbsent(groupId, x -> new HashMap<>());
                    final Long existingOffset = offsetsByTp.get(tp);
                    if (existingOffset == null || existingOffset <= lastCommittedOffset) {
                        offsetsByTp.put(tp, lastCommittedOffset);
                    } else {
                        System.err.println("Reading the files in wrong order. LastCommittedOffset = " + lastCommittedOffset + ", existingOffset: " + existingOffset);
                    }
                }
//                else {
                // KEY => metadata::console-consumer-65398
                // payload => consumer:range:1:{consumer-1-59733853-abe6-4887-aff2-feb9e737069b=[test-0, test-1, test-2]}

                // skip these messages as they are group metadata
                // System.out.println("Key: " + key + ", payload: " + payload);
//                }

            }
//            validBytes += MessageSet.entrySize(shallowMessageAndOffset.message());
        }
    }

}
