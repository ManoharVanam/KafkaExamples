package com.manu.kafka.largemessages;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ByteExample {

    public static void main(String s[]) throws FileNotFoundException {

//        byte[] ba = new byte[100];
//        String uuidValue = UUID.randomUUID().toString();
//        System.out.println(uuidValue);
//        byte[] uuid = uuidValue.getBytes(StandardCharsets.UTF_16);
//        System.out.println(uuid);
//        ByteBuffer bf = ByteBuffer.wrap(ba);
//        bf.putInt(uuid.length);
//        bf.put(uuid);
//        bf.putShort((short) 10);
//        bf.putShort((short) 10);
//
//
//        System.out.println(new String(uuid, StandardCharsets.UTF_16));
//
//        printDetails(bf);

        byte[] bytes = FileUtils.readFileInList("/Users/mvanam/Documents/kafkaTest/log.log");
        List<byte[]> list = new ArrayList<byte[]>();
        list.add(bytes);
        list.add(bytes);
        for(int i=1; i<250;i++ ){

            FileUtils.writeFile(list,"/Users/mvanam/Documents/kafkaTest/log" + i +".log");
        }

    }

    private static void printDetails(ByteBuffer bf) {
        bf.position(0);
        byte[] dest = new byte[bf.getInt()];
        bf.get(dest);
        System.out.println("uuid" + new String(dest, StandardCharsets.UTF_16));
        System.out.println("index " + new Short(bf.getShort()));
        System.out.println("chunks " + bf.getShort());
    }
}
