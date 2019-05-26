package com.manu.kafka.largemessages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Global map to store all consumed messaage chunks
 */
public class GlobalConsumerMap {
    public static volatile GlobalConsumerMap map;
    Map<String, Message> globalMap = new HashMap<>();

    private GlobalConsumerMap() {

    }

    public static GlobalConsumerMap getInstance() {
        if (map == null) {
            synchronized (GlobalConsumerMap.class) {
                if (map == null) {
                    map = new GlobalConsumerMap();
                }
            }
        }
        return map;
    }


    public boolean addMessage(String id, byte[] byteMsg, Short index, Short chunks) {
        if (globalMap.get(id) == null) {
            globalMap.put(id, new Message(byteMsg, index, chunks));
        } else {
            globalMap.get(id).addMessage(byteMsg, index, chunks);
        }
        return globalMap.get(id).status;
    }

    public Message getMessage(String id) {
        return globalMap.get(id);
    }

    public void removeMessage(String id) {
        globalMap.remove(id);
    }

    class Message {
        boolean status = false;

        public List<byte[]> getMessages() {
            return messages;
        }

        //    int chunks  = 0;
        List<byte[]> messages = new ArrayList<byte[]>();

        Message(byte[] msg, int index, int maxIndex) {
            add(msg, index, maxIndex);
        }

        public void addMessage(byte[] msg, int index, int maxIndex) {
            add(msg, index, maxIndex);
        }

        private void add(byte[] msg, int index, int maxIndex) {
            System.out.println(msg);
            messages.add(index, msg);
//        chunks ++;
//        messages.add(index, msg);
            if (messages.size() == maxIndex) {
                status = true;
            }
        }

        public boolean getStatus() {
            return status;
        }


    }
}
