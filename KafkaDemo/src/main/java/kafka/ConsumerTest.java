package kafka;


import kafka.log.FileMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import scala.collection.Iterator;

import java.io.File;

public class ConsumerTest {

    public static void main(String[] args) {
        System.out.println("manu");
        ConsumerTest test = new ConsumerTest();
        test.execute("/Users/mvanam/Downloads/00000000000000000000.log");


    }

    private void execute(String file) {

        boolean isDeepIteration = true;
        long validBytes = 0L;
        boolean printContents = true;
        FileMessageSet messageSet = new FileMessageSet(new File(file), true);
        Iterator<MessageAndOffset> shallowIterator = messageSet.iterator();
        while (shallowIterator.hasNext()) {
            MessageAndOffset shallowMessageAndOffset = shallowIterator.next();
            Iterator<MessageAndOffset> itr = KafkaTest.getIterator(shallowMessageAndOffset, isDeepIteration);
            while (itr.hasNext()) {
                MessageAndOffset messageAndOffset = itr.next();
                Message msg = messageAndOffset.message();
                System.out.println("offset: " + messageAndOffset.offset() + " position: " + validBytes + " isvalid: " + msg.isValid() +
                        " payloadsize: " + msg.payloadSize() + " magic: " + msg.magic() +
                        " compresscodec: " + msg.compressionCodec() + " crc: " + msg.checksum());
                if (msg.hasKey())
                    System.out.println(" keysize: " + msg.keySize());
                if (printContents) {
                    KafkaTest.executePrint(msg);
                }
                System.out.println();
            }
            validBytes += MessageSet.entrySize(shallowMessageAndOffset.message());
        }
    }

}
